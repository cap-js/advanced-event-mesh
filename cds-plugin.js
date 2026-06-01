const cds = require('@sap/cds')
const CDS_8 = cds.version.split('.')[0] < 9

const solace = require('solclientjs')
const EventEmitter = require('events')
const https = require('https')

const AEM = 'SAP Integration Suite, advanced event mesh'
const AEM_VAL = `${AEM} with plan "aem-validation-service"`
const UPS_FORMAT = `{
  "authentication-service": {
    "tokenendpoint": "https://<ias host>/oauth2/token",
    "clientid": "<client id>",
    "clientsecret": "<client secret>"
  },
  "endpoints": {
    "advanced-event-mesh": {
      "uri": "https://<broker host>:<port>",
      "smf_uri": "wss://<broker host>:<port>"
    }
  },
  "vpn": "<vpn>"
}`

const _getCredsFromVcap = test => {
  const vcap = process.env.VCAP_SERVICES && JSON.parse(process.env.VCAP_SERVICES)
  if (!vcap) throw new Error('No VCAP_SERVICES in process environment')
  for (const name in vcap) {
    const srv = vcap[name][0]
    if (test(srv)) return srv.credentials
  }
}

const _validateAndFetchEndpoints = creds => {
  const MSG = `Missing or malformed credentials for ${AEM}.\n\nBind your app to a user-provided service with name "advanced-event-mesh" and credentials in the following format:\n${UPS_FORMAT}`

  if (!creds || !creds['authentication-service'] || !creds.endpoints || !creds.vpn) throw new Error(MSG)

  const auth_srv = creds['authentication-service']
  if ((!auth_srv.tokenendpoint && !auth_srv['service-label']) || (auth_srv.tokenendpoint && auth_srv['service-label']))
    throw new Error(MSG)
  if (
    auth_srv.tokenendpoint &&
    (!auth_srv.clientid || (!auth_srv.clientsecret && (!auth_srv.certificate || !auth_srv.key)))
  ) {
    throw new Error(MSG)
  }
  if (auth_srv['service-label'] && !auth_srv.api) throw new Error(MSG)

  const first = creds.endpoints[Object.keys(creds.endpoints)[0]]
  if (!first || !first.uri || !first.smf_uri) throw new Error(MSG)

  return first
}

function _fetchToken({ tokenendpoint, clientid, clientsecret, certificate: cert, key, api }) {
  return new Promise((resolve, reject) => {
    const body = { grant_type: 'client_credentials', response_type: 'token', client_id: clientid }
    if (api) body.resource = [`urn:sap:identity:application:provider:name:${api}`]
    const options = { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' } }
    // certificate or secret?
    if (cert) options.agent = new https.Agent({ cert, key })
    else body.client_secret = clientsecret
    const data = Object.keys(body).reduce((acc, cur) => ((acc += (acc ? '&' : '') + cur + '=' + body[cur]), acc), '')
    const req = https.request(tokenendpoint, options, res => {
      const chunks = []
      res.on('data', chunk => chunks.push(chunk))
      res.on('end', () => {
        const { statusCode: code, statusMessage: msg } = res
        let body = Buffer.concat(chunks).toString()
        if (res.headers['content-type']?.match(/json/)) body = JSON.parse(body)
        if (res.statusCode >= 400) {
          reject(new Error(`Token request failed with${msg ? `: ${code} - ${msg}` : ` status ${code}`}`))
        } else {
          resolve(body)
        }
      })
    })
    req.on('error', reject)
    req.write(data)
    req.end()
  })
}

const _validateBroker = async (mgmt_uri, subaccountId) => {
  // via VCAP_SERVICES to avoid specifying _another_ cds.requires service
  const creds = _getCredsFromVcap(srv => srv.plan === 'aem-validation-service-plan')

  if (
    !creds ||
    !creds.handshake ||
    !creds.handshake.oa2 ||
    !creds.handshake.oa2.clientid ||
    !creds.handshake.oa2.clientsecret ||
    !creds.handshake.oa2.tokenendpoint ||
    !creds.handshake.uri ||
    !creds.serviceinstanceid
  ) {
    throw new Error(`Missing credentials for ${AEM_VAL}.\n\nYou need to create a service binding.`)
  }

  const { access_token: validationToken } = await _fetchToken(creds.handshake.oa2)

  const body = { hostName: mgmt_uri.match(/https?:\/\/(.*):.*/)[1] }
  if (subaccountId) body.subaccountId = subaccountId

  const res = await fetch(creds.handshake.uri, {
    method: 'POST',
    body: JSON.stringify(body),
    headers: { Authorization: 'Bearer ' + validationToken }
  })
  if (res.status === 500) throw new Error(`${AEM}: Error during VMR validation: 500 - ${res.statusText}`)
  if (res.status !== 200) throw new Error(`${AEM}: The provided VMR is not provisioned via AEM`)
}

const _JSONorString = string => {
  try {
    return JSON.parse(string)
  } catch {
    return string
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

    const { uri, smf_uri } = _validateAndFetchEndpoints(this.options.credentials)
    const mgmt_uri = uri + '/SEMP/v2/config'
    await _validateBroker(mgmt_uri, this.options.subaccountId)

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

    const vpn = this.options.credentials.vpn
    const queueName = this.options.queue.name
    this._queues_uri = `${mgmt_uri}/msgVpns/${vpn}/queues`
    this._subscriptions_uri = `${this._queues_uri}/${encodeURIComponent(queueName)}/subscriptions`

    let auth_srv = this.options.credentials['authentication-service']
    if ('service-label' in auth_srv) {
      const creds = _getCredsFromVcap(srv => srv.label === auth_srv['service-label'])
      auth_srv = { ...auth_srv, ...creds }
      auth_srv.tokenendpoint ??= auth_srv.url + '/oauth2/token'
    }
    const { access_token, expires_in } = await _fetchToken(auth_srv)
    this.token = access_token
    this.token_expires_in = expires_in

    const solclientFactoryProperties = Object.assign(
      {
        logLevel: this.options.logLevel != null ? this.options.logLevel : Math.max(this.LOG.level - 1, 1),
        logger: Object.assign(this.LOG, { fatal: this.LOG.error }),
        profile: solace.SolclientFactoryProfiles.version10
      },
      this.options.clientFactory
    )
    solace.SolclientFactory.init(new solace.SolclientFactoryProperties(solclientFactoryProperties))

    const sessionProperties = Object.assign(
      { url: smf_uri, vpnName: this.options.credentials.vpn, accessToken: this.token },
      this.options.session
    )
    this.session = solace.SolclientFactory.createSession(sessionProperties)

    this.session.on(solace.SessionEventCode.ACKNOWLEDGED_MESSAGE, sessionEvent => {
      this._eventAck.emit(sessionEvent.correlationKey)
    })
    this.session.on(solace.SessionEventCode.REJECTED_MESSAGE_ERROR, sessionEvent => {
      this._eventRej.emit(sessionEvent.correlationKey, sessionEvent)
    })

    const _scheduleUpdateToken = waitingTime => {
      waitingTime ??= Math.max(this.token_expires_in - 10, 0) * 1000
      setTimeout(async () => {
        this.LOG._info && this.LOG.info('Fetching fresh token')
        try {
          const { access_token, expires_in } = await _fetchToken(auth_srv)
          this.token = access_token
          this.token_expires_in = expires_in
          this.session.updateAuthenticationOnReconnect({ accessToken: this.token })
          _scheduleUpdateToken()
        } catch (error) {
          this.LOG.error('Could not fetch fresh token:', error)
          _scheduleUpdateToken(10 * 1000)
        }
      }, waitingTime).unref()
    }

    return new Promise((resolve, reject) => {
      this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
        _scheduleUpdateToken()
        resolve()
      })
      this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, sessionEvent => {
        this.LOG.error('CONNECT_FAILED_ERROR:', sessionEvent)
        reject(sessionEvent)
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

    if (!this.options.skipManagement) {
      await this._createQueueM()
      await this._subscribeTopicsM()
    }

    this.options.consumer.queueDescriptor.name = this.options.queue.name

    this.messageConsumer = this.session.createMessageConsumer(this.options.consumer)
    this.messageConsumer.on(solace.MessageConsumerEventName.MESSAGE, async message => {
      const event = message.getDestination().getName()
      if (this.LOG._info) this.LOG.info('Received message', event)
      let payload
      if (message.getType() == solace.MessageType.TEXT) {
        payload = message.getSdtContainer().getValue()
      } else {
        payload = message.getBinaryAttachment()
      }
      const msg = this.normalizeIncomingMessage(payload)
      msg.event = event
      try {
        // NOTE: processInboundMsg doesn't exist in cds^8
        if (CDS_8) await this.tx({ user: cds.User.privileged }, tx => tx.emit(msg))
        else await this.processInboundMsg({ user: cds.User.privileged }, msg)
        message.acknowledge()
      } catch (e) {
        e.message = 'ERROR occurred in asynchronous event processing: ' + e.message
        this.LOG.error(e)
        // The error property `unrecoverable` is used for the outbox to mark unrecoverable errors.
        // We can use the same here to properly reject the message.
        if (
          e.unrecoverable &&
          this.options.consumer.requiredSettlementOutcomes.includes(solace.MessageOutcome.REJECTED)
        ) {
          return message.settle(solace.MessageOutcome.REJECTED)
        }
        if (this.options.consumer.requiredSettlementOutcomes.includes(solace.MessageOutcome.FAILED))
          return message.settle(solace.MessageOutcome.FAILED)
        // Nothing else we can do
        message.acknowledge()
      }
    })
    return new Promise((resolve, reject) => {
      this.messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
        this.LOG._info && this.LOG.info('Consumer connected')
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

  /**
   * Some messaging systems don't adhere to the standard that the payload has a data property 
   * For these cases, we interpret the whole payload as data
   */ 
  _normalizeIncomingMessage(message) {
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

    if (CDS_8) return { data, headers, inbound: true }
    return { data, headers }
  }
}
