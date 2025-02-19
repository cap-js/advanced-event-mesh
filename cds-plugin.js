const solace = require('solclientjs')
const EventEmitter = require('events')

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

module.exports = class AdvancedEventMesh extends cds.MessagingService {
  async init() {
    await super.init()

    this._eventAck = new EventEmitter() // for reliable messaging
    this._eventRej = new EventEmitter() // for reliable messaging

    cds.once('listening', () => {
      this.startListening()
    })

    const clientId = this.options.credentials.clientid
    const clientSecret = this.options.credentials.clientsecret
    const tokenEndpoint = this.options.credentials.tokenendpoint
    const vpn = this.options.credentials.vpn
    const uri = this.options.credentials.uri

    if (!clientId || !clientSecret || !tokenEndpoint || !vpn || !uri)
      throw new Error(
        'Missing credentials for SAP Advanced Event Mesh.\n\nProvide a user-provided service with name `advanced-event-mesh` and credentials { clientid, clientsecret, tokenendpoint, vpn, uri }.'
      )

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

    this.options.queue.queueDescriptor.name = prepareQueueName(
      this.options.queue.name || this.options.queue.queueDescriptor.name || '$appId'
    )
    delete this.options.queue.name

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
      try {
        this.session.connect()
      } catch (error) {
        reject(error)
      }
      this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
        resolve()
      })
      this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, e => {
        reject(e)
      })
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

    //await this._createQueue()
    await this._createQueueManagement()
    await this._subscribeTopics()

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
        message.settle(solace.MessageOutcome.FAILED)
      }
    })
  }

  async _createQueue() {
    if (this.LOG._info) this.LOG.info('Creating queue', this.options.queue.queueDescriptor.name)
    return new Promise((resolve, reject) => {
      this.messageConsumer = this.session.createMessageConsumer(this.options.queue)

      this.messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
        if (this.LOG._info) this.LOG.info('Queue created', this.options.queue.queueDescriptor.name)
        resolve()
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.DOWN, _event => {
        this.LOG.error('Queue down', this.options.queue.queueDescriptor.name)
        reject(new Error('Message Consumer failed to start.'))
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, _event => {
        this.LOG.error('Could not connect to queue', this.options.queue.queueDescriptor.name)
        reject(new Error('Message Consumer connection failed.'))
      })
      this.messageConsumer.connect()
    })
  }

  async _createQueueManagement() {
    try {
      const queueConfig = (this.queueConfig && { ...this.queueConfig }) || {}
      queueConfig.queueName = this.options.queue.queueDescriptor.name
      // queueConfig.owner = this.options.owner
      queueConfig.ingressEnabled = true
      queueConfig.egressEnabled = true

      console.log('url:', this.options.credentials.management.uri + `/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues`)
      console.log('queueConfig', queueConfig)
      console.log('token', this.token)
      const res = await fetch(this.options.credentials.management.uri + `/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues`, {
        method: 'POST',
        body: JSON.stringify(queueConfig),
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
      const error = new Error(`Queue "${this.options.queue.queueDescriptor.name}" could not be created`)
      error.code = 'CREATE_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: this.options.queue.queueDescriptor.name}
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }
  async _subscribeTopicsManagement() {
    try {
      const queueConfig = (this.queueConfig && { ...this.queueConfig }) || {}
      queueConfig.queueName = this.options.queue.queueDescriptor.name
      // queueConfig.owner = this.options.owner
      queueConfig.ingressEnabled = true
      queueConfig.egressEnabled = true

      const res = await fetch(this.options.credentials.management.uri + `/SEMP/v2/config/msgVpns/${this.options.credentials.vpn}/queues`, {
        method: 'POST',
        body: JSON.stringify(queueConfig),
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
      const error = new Error(`Queue "${this.options.queue.queueDescriptor.name}" could not be created`)
      error.code = 'CREATE_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: this.options.queue.queueDescriptor.name}
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async _subscribeTopics() {
    const topics = [...this.subscribedTopics].map(kv => kv[0])
    return new Promise((resolve, reject) => {
      const subscribed = []
      this.messageConsumer.on(solace.MessageConsumerEventName.SUBSCRIPTION_OK, sessionEvent => {
        subscribed.push(sessionEvent.correlationKey)
        if (subscribed.length === topics.length) resolve()
      })
      this.messageConsumer.on(solace.MessageConsumerEventName.SUBSCRIPTION_ERROR, sessionEvent => {
        reject(sessionEvent.reason)
      })
      for (const topic of topics) {
        this.messageConsumer.addSubscription(
          solace.SolclientFactory.createTopicDestination(topic),
          topic, // correlation key as topic name
          10000 // 10 seconds timeout for this operation
        )
      }
    })
  }
}
