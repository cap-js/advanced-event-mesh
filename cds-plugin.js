const solace = require('solclientjs')
const EventEmitter = require('events')

const _JSONorString = string => {
  try {
    return JSON.parse(string)
  } catch {
    return string
  }
}

const NEED_CRED =
  'Missing credentials for SAP Advanced Event Mesh.\n\nProvide a user-provided service with name `advanced-event-mesh` and credentials { clientid, clientsecret, tokenendpoint, vpn, uri, management_uri }.'

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

module.exports = class AdvancedEventMesh extends cds.MessagingService {
  async init() {
    await super.init()

    this._eventAck = new EventEmitter() // for reliable messaging
    this._eventRej = new EventEmitter() // for reliable messaging

    cds.once('listening', () => {
      this.startListening()
    })

    if (!this.options.credentials) throw new Error(NEED_CRED)

    const clientId = this.options.credentials.clientid
    const clientSecret = this.options.credentials.clientsecret
    const tokenEndpoint = this.options.credentials.tokenendpoint
    const vpn = this.options.credentials.vpn
    const uri = this.options.credentials.uri

    if (!clientId || !clientSecret || !tokenEndpoint || !vpn || !uri) throw new Error(NEED_CRED)

    const optionsApp = require('@sap/cds/libx/_runtime/common/utils/vcap.js') // TODO: streamline
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

    const resp = await fetch(tokenEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret // scope?
      })
    }).then(x => x.json())

    this.token = resp.access_token

    const factoryProps = new solace.SolclientFactoryProperties()
    factoryProps.profile = solace.SolclientFactoryProfiles.version10
    solace.SolclientFactory.init(factoryProps)
    solace.SolclientFactory.setLogLevel(this.options.logLevel)

    this.session = solace.SolclientFactory.createSession(
      Object.assign(
        {
          url: uri,
          vpnName: vpn,
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
      this._eventRej.once(correlationKey, _sessionEvent => {
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
        if (this.options.consumer.requiredSettlementOutcomes.includes(solace.MessageOutcome.FAILED)) message.settle(solace.MessageOutcome.FAILED)
        else message.acknowledge()
      }
    })
    return new Promise((resolve, reject) => {
      this.messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
        if (this.LOG._info) this.LOG.info('Consumer connected')
        resolve()
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.DOWN, _event => {
        this.LOG.error('Queue down', this.options.queue.name)
        reject(new Error('Message Consumer failed to start.'))
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, _event => {
        this.LOG.error('Could not connect to queue', this.options.queue.name)
        reject(new Error('Message Consumer connection failed.'))
      })
      this.messageConsumer.connect()
    })
  }

  async _createQueueM() {
    try {
      //const queueConfig = (this.queueConfig && { ...this.queueConfig }) || {}
      //queueConfig.queueName = this.options.queue.name
      //// queueConfig.owner = this.options.owner
      //queueConfig.ingressEnabled = true
      //queueConfig.egressEnabled = true

      // name -> queueName
      const body = { ...this.options.queue }
      body.queueName ??= this.options.queue.name
      delete body.name

      // https://docs.solace.com/API-Developer-Online-Ref-Documentation/swagger-ui/software-broker/config/index.html#/msgVpn/createMsgVpnQueue
      const res = await fetch(
        `${this.options.credentials.management_uri}/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues`,
        {
          method: 'POST',
          body: JSON.stringify(body),
          headers: {
            accept: 'application/json',
            'content-type': 'application/json',
            encoding: 'utf-8',
            authorization: 'Bearer ' + this.token
          }
        }
      ).then(r => r.json())
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
      const res = await fetch(
        `${this.options.credentials.management_uri}/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions`,
        {
          headers: {
            accept: 'application/json',
            authorization: 'Bearer ' + this.token
          }
        }
      ).then(r => r.json())
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
    this.LOG._info && this.LOG.info('Create subscription', { topic: topicPattern, queue: queueName })
    try {
      const res = await fetch(
        `${this.options.credentials.management_uri}/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions`,
        {
          method: 'POST',
          body: JSON.stringify({ subscriptionTopic: topicPattern }),
          headers: {
            accept: 'application/json',
            'content-type': 'application/json',
            encoding: 'utf-8',
            authorization: 'Bearer ' + this.token
          }
        }
      ).then(r => r.json())
      if (res.meta?.error && res.meta.error.status !== 'ALREADY_EXISTS') throw res.meta.error
      if (res.statusCode === 201) return true
    } catch (e) {
      const error = new Error(`Subscription "${topicPattern}" could not be added to queue "${queueName}"`)
      error.code = 'CREATE_SUBSCRIPTION_FAILED'
      error.target = { kind: 'SUBSCRIPTION', queue: queueName, topic: topicPattern }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async _deleteSubscriptionM(topicPattern) {
    const queueName = this.options.queue.name
    this.LOG._info && this.LOG.info('Delete subscription', { topic: topicPattern, queue: queueName })
    try {
      await fetch(
        `${this.options.credentials.management_uri}/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions/${encodeURIComponent(topicPattern)}`,
        {
          method: 'DELETE',
          headers: {
            accept: 'application/json',
            authorization: 'Bearer ' + this.token
          }
        }
      ).then(r => r.json())
    } catch (e) {
      const error = new Error(`Subscription "${topicPattern}" could not be deleted from queue "${queueName}"`)
      error.code = 'DELETE_SUBSCRIPTION_FAILED'
      error.target = { kind: 'SUBSCRIPTION', queue: queueName, topic: topicPattern }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }
}
