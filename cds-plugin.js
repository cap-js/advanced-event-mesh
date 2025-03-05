const cds = require('@sap/cds')

const solace = require('solclientjs')

const EventEmitter = require('events')
const { hostname } = require('os')

const _CREDS_ERROR = `Missing or malformed credentials for SAP Integration Suite, advanced event mesh.

Provide a user-provided service with name "advanced-event-mesh" and credentials in the following format:
{
  "authentication-service": {
    "token_endpoint": "https://<host>/oauth2/token",
    "clientid": "<clientid>",
    "clientsecret": "<clientsecret>"
  },
  "endpoints": {
    "eventing-endpoint": {
      "uri": "https://<host>:443"
    },
    "management-endpoint": {
      "uri": "https://<host>:943/SEMP/v2/config"
    }
  },
  "vpn": "<vpn>"
}`

const _JSONorString = string => {
  try {
    return JSON.parse(string)
  } catch {
    return string
  }
}

// Some messaging systems don't adhere to the standard that the payload has a `data` property.
// For these cases, we interpret the whole payload as `data`.
const normalizeIncomingMessage = message => {
  const _payload = typeof message === 'object' ? message : _JSONorString(message)
  let data, headers
  if (typeof _payload === 'object' && 'data' in _payload) {
    data = _payload.data
    headers = { ..._payload }
    delete headers.data
  } else {
    data = _payload
    headers = {}
  }

  return {
    data,
    headers,
    inbound: true
  }
}

const getAppMetadata = () => {
  // NOT official, but consistent with Event Mesh!
  const appMetadata = cds.env.app

  if (appMetadata) {
    return {
      appID: appMetadata.id,
      appName: appMetadata.name
    }
  }

  const vcapApplication = process.env.VCAP_APPLICATION && JSON.parse(process.env.VCAP_APPLICATION)

  return {
    appID: vcapApplication && vcapApplication.application_id,
    appName: vcapApplication && vcapApplication.application_name
  }
}

module.exports = class AdvancedEventMesh extends cds.MessagingService {
  async init() {
    await super.init()

    if (
      !this.options.credentials ||
      !this.options.credentials['authentication-service'] ||
      !this.options.credentials['authentication-service'].token_endpoint ||
      !this.options.credentials['authentication-service'].clientid ||
      !this.options.credentials['authentication-service'].clientsecret ||
      !this.options.credentials.endpoints ||
      !this.options.credentials.endpoints['eventing-endpoint'] ||
      !this.options.credentials.endpoints['eventing-endpoint'].uri ||
      !this.options.credentials.endpoints['management-endpoint'] ||
      !this.options.credentials.endpoints['management-endpoint'].uri ||
      !this.options.credentials.vpn
    ) {
      throw new Error(_CREDS_ERROR)
    }

    // I'd rather not require users to specify _another_ cds.requires service
    const vcredentials = (() => {
      const vcap = process.env.VCAP_SERVICES && JSON.parse(process.env.VCAP_SERVICES)
      for (const name in vcap) {
        const srv = vcap[name][0]
        if (srv.plan === 'aem-validation-service-plan') {
          return srv.credentials
        }
      }
    })()
    if (
      !vcredentials ||
      !vcredentials.handshake ||
      !vcredentials.handshake.oa2 ||
      !vcredentials.handshake.oa2.clientid ||
      !vcredentials.handshake.oa2.clientsecret ||
      !vcredentials.handshake.oa2.tokenendpoint ||
      !vcredentials.handshake.uri ||
      !vcredentials.serviceinstanceid
    )
      throw new Error(
        'Missing credentials for SAP Integration Suite, advanced event mesh with plan `aem-validation-service`.\n\nYou need to create a service binding.'
      )

    const validationTokenRes = await fetch(vcredentials.handshake.oa2.tokenendpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: vcredentials.handshake.oa2.clientid,
        client_secret: vcredentials.handshake.oa2.clientsecret
      })
    }).then(r => r.json())

    if (validationTokenRes.error)
      throw new Error(
        'Could not fetch token for SAP Integration Suite, advanced event mesh with plan `aem-validation-service`: ' +
          validationTokenRes.error_description
      )
    const validationToken = validationTokenRes.access_token

    const validatationRes = await fetch(vcredentials.handshake.uri, {
      method: 'POST',
      body: JSON.stringify({
        hostName: this.options.credentials.endpoints['management-endpoint'].uri.match(/https:\/\/(.*):.*/)[1]
      }),
      headers: {
        Authorization: 'Bearer ' + validationToken
      }
    })

    if (validatationRes.status !== 200)
      throw new Error('SAP Integration Suite, advanced event mesh: The provided VMR is not provisioned via AEM')

    this._eventAck = new EventEmitter() // for reliable messaging
    this._eventRej = new EventEmitter() // for reliable messaging

    cds.once('listening', () => {
      this.startListening()
    })

    const optionsApp = getAppMetadata()
    const appId = () => {
      const appName = optionsApp.appName || 'CAP'
      const appID = optionsApp.appID || '00000000'
      const shrunkAppID = appID.substring(0, 4)
      return `${appName}/${shrunkAppID}`
    }

    const prepareQueueName = queueName => {
      return queueName.replace(/\$appId/g, appId())
    }

    this.options.queue.name = prepareQueueName(this.options.queue.queueName || this.options.queue.name) // latter is more similar to other brokers
    delete this.options.queue.queueName

    const mgmt_uri = this.options.credentials.endpoints['management-endpoint'].uri
    const vpn = this.options.credentials.vpn
    const queueName = this.options.queue.name
    this._queues_uri = `${mgmt_uri}/msgVpns/${vpn}/queues`
    this._subscriptions_uri = `${this._queues_uri}/${encodeURIComponent(queueName)}/subscriptions`

    const { token_endpoint, clientid, clientsecret } = this.options.credentials['authentication-service']
    const res = await fetch(token_endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientid,
        client_secret: clientsecret // scope?
      })
    }).then(r => r.json())

    if (res.error) {
      throw new Error('Could not fetch token for SAP Integration Suite, advanced event mesh: ' + res.error_description)
    }

    this.token = res.access_token

    const factoryProps = new solace.SolclientFactoryProperties()
    factoryProps.profile = solace.SolclientFactoryProfiles.version10
    solace.SolclientFactory.init(factoryProps)
    solace.SolclientFactory.setLogLevel(this.options.logLevel)

    this.session = solace.SolclientFactory.createSession(
      Object.assign(
        {
          url: this.options.credentials.endpoints['eventing-endpoint'].uri,
          vpnName: this.options.credentials.vpn,
          accessToken: this.token
        },
        this.options.session
      )
    )

    this.session.on(solace.SessionEventCode.ACKNOWLEDGED_MESSAGE, sessionEvent => {
      this._eventAck.emit(sessionEvent.correlationKey)
    })
    this.session.on(solace.SessionEventCode.REJECTED_MESSAGE_ERROR, sessionEvent => {
      this._eventRej.emit(sessionEvent.correlationKey, sessionEvent)
    })

    return new Promise((resolve, reject) => {
      this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
        resolve()
      })
      this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, e => {
        reject(e)
      })
      try {
        this.session.connect()
      } catch (error) {
        reject(error)
      }
    })
  }

  async handle(msg) {
    if (msg.inbound) return super.handle(msg)
    const _msg = this.message4(msg)
    this.LOG._info && this.LOG.info('Emit', { topic: _msg.event })
    const message = solace.SolclientFactory.createMessage()
    message.setDestination(solace.SolclientFactory.createTopicDestination(msg.event))
    message.setBinaryAttachment(JSON.stringify({ data: _msg.data, ...(_msg.headers || {}) }))
    message.setDeliveryMode(solace.MessageDeliveryModeType.PERSISTENT)
    const correlationKey = cds.utils.uuid()
    message.setCorrelationKey(correlationKey)
    return new Promise((resolve, reject) => {
      this._eventAck.once(correlationKey, () => {
        this._eventRej.removeAllListeners(correlationKey)
        resolve()
      })
      this._eventRej.once(correlationKey, () => {
        this._eventAck.removeAllListeners(correlationKey)
        reject()
      })
      this.session.send(message)
    })
  }

  async startListening() {
    if (!this._listenToAll.value && !this.subscribedTopics.size) return

    await this._createQueueM()
    await this._subscribeTopicsM()

    this.options.consumer.queueDescriptor.name = this.options.queue.name

    this.messageConsumer = this.session.createMessageConsumer(this.options.consumer)
    this.messageConsumer.on(solace.MessageConsumerEventName.MESSAGE, async message => {
      const event = message.getDestination().getName()
      if (this.LOG._info) this.LOG.info('Received message', event)
      const msg = normalizeIncomingMessage(message.getBinaryAttachment())
      msg.event = event
      try {
        await this.tx({ user: cds.User.privileged }, tx => tx.emit(msg))
        message.acknowledge()
      } catch (e) {
        e.message = 'ERROR occurred in asynchronous event processing: ' + e.message
        this.LOG.error(e)
        // The error property `unrecoverable` is used for the outbox to mark unrecoverable errors.
        // We can use the same here to properly reject the message.
        if (
          e.unrecoverable &&
          this.options.consumer.requiredSettlementOutcomes.includes(solace.MessageOutcome.REJECTED)
        )
          return message.settle(solace.MessageOutcome.REJECTED)
        if (this.options.consumer.requiredSettlementOutcomes.includes(solace.MessageOutcome.FAILED))
          return message.settle(solace.MessageOutcome.FAILED)
        // Nothing else we can do
        message.acknowledge()
      }
    })
    return new Promise((resolve, reject) => {
      this.messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
        if (this.LOG._info) this.LOG.info('Consumer connected')
        resolve()
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.DOWN, () => {
        this.LOG.error('Queue down', this.options.queue.name)
        reject(new Error('Message Consumer failed to start.'))
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, () => {
        this.LOG.error('Could not connect to queue', this.options.queue.name)
        reject(new Error('Message Consumer connection failed.'))
      })
      this.messageConsumer.connect()
    })
  }

  async _createQueueM() {
    try {
      // name -> queueName
      const body = { ...this.options.queue }
      body.queueName ??= this.options.queue.name
      delete body.name

      // https://docs.solace.com/API-Developer-Online-Ref-Documentation/swagger-ui/software-broker/config/index.html#/msgVpn/createMsgVpnQueue
      const res = await fetch(this._queues_uri, {
        method: 'POST',
        body: JSON.stringify(body),
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          encoding: 'utf-8',
          authorization: 'Bearer ' + this.token
        }
      }).then(r => r.json())
      if (res.meta?.error && res.meta.error.status !== 'ALREADY_EXISTS') throw res.meta.error
      if (res.statusCode === 201) return true
    } catch (e) {
      const error = new Error(`Queue "${this.options.queue.name}" could not be created`)
      error.code = 'CREATE_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: this.options.queue.name }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }
  async _subscribeTopicsM() {
    const existingTopics = await this._getSubscriptionsM()
    const topics = [...this.subscribedTopics].map(kv => kv[0])
    const newTopics = []
    for (const t of topics) if (!existingTopics.includes(t)) newTopics.push(t)
    const toBeDeletedTopics = []
    for (const t of existingTopics) if (!topics.includes(t)) toBeDeletedTopics.push(t)
    await Promise.all(toBeDeletedTopics.map(t => this._deleteSubscriptionM(t)))
    await Promise.all(newTopics.map(t => this._createSubscriptionM(t)))
  }

  async _getSubscriptionsM() {
    const queueName = this.options.queue.name
    this.LOG._info && this.LOG.info('Get subscriptions', { queue: queueName })
    try {
      const res = await fetch(this._subscriptions_uri, {
        headers: {
          accept: 'application/json',
          authorization: 'Bearer ' + this.token
        }
      }).then(r => r.json())
      if (res.meta?.error) throw res.meta.error
      return res.data.map(t => t.subscriptionTopic)
    } catch (e) {
      const error = new Error(`Subscriptions for "${queueName}" could not be retrieved`)
      error.code = 'GET_SUBSCRIPTIONS_FAILED'
      error.target = { kind: 'SUBSCRIPTION', queue: queueName }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async _createSubscriptionM(topicPattern) {
    const queueName = this.options.queue.name
    this.LOG._info &&
      this.LOG.info('Create subscription', {
        topic: topicPattern,
        queue: queueName
      })
    try {
      const res = await fetch(this._subscriptions_uri, {
        method: 'POST',
        body: JSON.stringify({ subscriptionTopic: topicPattern }),
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          encoding: 'utf-8',
          authorization: 'Bearer ' + this.token
        }
      }).then(r => r.json())
      if (res.meta?.error && res.meta.error.status !== 'ALREADY_EXISTS') throw res.meta.error
      if (res.statusCode === 201) return true
    } catch (e) {
      const error = new Error(`Subscription "${topicPattern}" could not be added to queue "${queueName}"`)
      error.code = 'CREATE_SUBSCRIPTION_FAILED'
      error.target = {
        kind: 'SUBSCRIPTION',
        queue: queueName,
        topic: topicPattern
      }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async _deleteSubscriptionM(topicPattern) {
    const queueName = this.options.queue.name
    this.LOG._info &&
      this.LOG.info('Delete subscription', {
        topic: topicPattern,
        queue: queueName
      })
    try {
      await fetch(`${this._subscriptions_uri}/${encodeURIComponent(topicPattern)}`, {
        method: 'DELETE',
        headers: {
          accept: 'application/json',
          authorization: 'Bearer ' + this.token
        }
      }).then(r => r.json())
    } catch (e) {
      const error = new Error(`Subscription "${topicPattern}" could not be deleted from queue "${queueName}"`)
      error.code = 'DELETE_SUBSCRIPTION_FAILED'
      error.target = {
        kind: 'SUBSCRIPTION',
        queue: queueName,
        topic: topicPattern
      }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }
}
