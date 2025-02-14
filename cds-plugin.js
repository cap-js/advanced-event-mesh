const solace = require('solclientjs')
const EventEmitter = require('node:events')

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
    // TODO: Error handling
    console.log({ clientId, clientSecret, tokenEndpoint })

    if (this.options.queue) {
      const queueConfig = { ...this.options.queue }
      delete queueConfig.name
      if (Object.keys(queueConfig).length) this.queueConfig = queueConfig
    }

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
    this.queueName = prepareQueueName(this.options.queue?.name || '$appId')

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

    const token = resp.access_token
    console.log(token)

    const factoryProps = new solace.SolclientFactoryProperties()
    factoryProps.profile = solace.SolclientFactoryProfiles.version10
    solace.SolclientFactory.init(factoryProps)
    solace.SolclientFactory.setLogLevel(solace.LogLevel.ERROR)
    this.session = solace.SolclientFactory.createSession({
      url: uri,
      vpnName: vpn,
      authenticationScheme: solace.AuthenticationScheme.OAUTH2,
      accessToken: token,
      publisherProperties: {
        acknowledgeMode: solace.MessagePublisherAcknowledgeMode.PER_MESSAGE
      },
      // userName: this.options.user,
      // password: this.options.password,
      connectRetries: -1
    })

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

    await this._createQueue()
    await this._subscribeTopics()

    this.messageConsumer.on(solace.MessageConsumerEventName.MESSAGE, async message => {
      console.log('received msg')
      const msg = normalizeIncomingMessage(message.getBinaryAttachment())
      msg.event = message.getDestination().getName()
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
    if (this.LOG._info) this.LOG.info('Creating queue', this.queueName)
    return new Promise((resolve, reject) => {
      this.messageConsumer = this.session.createMessageConsumer({
        // solace.MessageConsumerProperties
        queueDescriptor: { name: this.queueName, type: solace.QueueType.QUEUE },
        acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT, // Enabling Client ack
        requiredSettlementOutcomes: [solace.MessageOutcome.FAILED, solace.MessageOutcome.REJECTED],
        createIfMissing: true
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
        if (this.LOG._info) this.LOG.info('Queue created', this.queueName)
        resolve()
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.DOWN, _event => {
        this.LOG.error('Queue down', this.queueName)
        reject(new Error('Message Consumer failed to start.'))
      })

      this.messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, _event => {
        this.LOG.error('Could not connect to queue', this.queueName)
        reject(new Error('Message Consumer connection failed.'))
      })
      this.messageConsumer.connect()
    })
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
        console.log('oh oh', sessionEvent.reason, 'for', sessionEvent.correlationKey)
        console.log(sessionEvent)
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
