const EnterpriseMessagingShared = require('@sap/cds/libx/_runtime/messaging/enterprise-messaging-shared.js')

const solace = require('solclientjs')

const requiredParams =
  'No proper credentials found for SAP Advanced Event Mesh.\n\nHint: You need to create a user-provided service (default name `advanced-event-mesh`)'

class AEMManagement {
  constructor({ optionsManagement, queueConfig, queueName, subscribedTopics, LOG }) {
    this.options = optionsManagement
    this.queueConfig = queueConfig
    this.queueName = queueName
    this.subscribedTopics = subscribedTopics
    this.LOG = LOG
  }
  async getQueue(queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Get queue', { queue: queueName })
    try {
      const res = await fetch(
        this.options.uri + `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues/${encodeURIComponent(queueName)}`,
        {
          headers: {
            accept: 'application/json',
            authorization: 'Basic ' + this.options.token
          }
        }
      ).then(r => r.json())
      if (res.meta?.error) throw res.meta.error
      return res.data
    } catch (e) {
      const error = new Error(`Queue "${queueName}" could not be retrieved`)
      error.code = 'GET_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: queueName }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async getQueues() {
    this.LOG._info && this.LOG.info('Get queues')
    try {
      const res = await fetch(this.options.uri + `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues`, {
        headers: {
          accept: 'application/json',
          authorization: 'Basic ' + this.options.token
        }
      }).then(r => r.json())
      if (res.meta?.error) throw res.meta.error
      return res.data
    } catch (e) {
      const error = new Error(`Queues could not be retrieved`)
      error.code = 'GET_QUEUES_FAILED'
      error.target = { kind: 'QUEUE' }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async createQueue(queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Create queue', { queue: queueName })
    try {
      const queueConfig = (this.queueConfig && { ...this.queueConfig }) || {}
      queueConfig.queueName = queueName
      queueConfig.owner = this.options.owner
      queueConfig.ingressEnabled = true
      queueConfig.egressEnabled = true
      if (queueConfig.deadMsgQueue)
        queueConfig.deadMsgQueue = queueConfig.deadMsgQueue.replace(/\$namespace/g, this.namespace)

      const res = await fetch(this.options.uri + `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues`, {
        method: 'POST',
        body: JSON.stringify(queueConfig),
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          encoding: 'utf-8',
          authorization: 'Basic ' + this.options.token
        }
      }).then(r => r.json())
      if (res.meta?.error && res.meta.error.status !== 'ALREADY_EXISTS') throw res.meta.error
      if (res.statusCode === 201) return true
    } catch (e) {
      const error = new Error(`Queue "${queueName}" could not be created`)
      error.code = 'CREATE_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: queueName }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async deleteQueue(queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Delete queue', { queue: queueName })
    try {
      await fetch(this.options.uri + `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues/${encodeURIComponent(queueName)}`, {
        method: 'DELETE',
        headers: {
          accept: 'application/json',
          authorization: 'Basic ' + this.options.token
        }
      }).then(r => r.json())
    } catch (e) {
      const error = new Error(`Queue "${queueName}" could not be deleted`)
      error.code = 'DELETE_QUEUE_FAILED'
      error.target = { kind: 'QUEUE', queue: queueName }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async getSubscriptions(queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Get subscriptions', { queue: queueName })
    try {
      const res = await fetch(
        this.options.uri +
          `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions`,
        {
          headers: {
            accept: 'application/json',
            authorization: 'Basic ' + this.options.token
          }
        }
      ).then(r => r.json())
      if (res.meta?.error) throw res.meta.error
      return res.data
    } catch (e) {
      const error = new Error(`Subscriptions for "${queueName}" could not be retrieved`)
      error.code = 'GET_SUBSCRIPTIONS_FAILED'
      error.target = { kind: 'SUBSCRIPTION', queue: queueName }
      error.reason = e
      this.LOG.error(error)
      throw error
    }
  }

  async createSubscription(topicPattern, queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Create subscription', { topic: topicPattern, queue: queueName })
    try {
      const res = await fetch(
        this.options.uri +
          `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions`,
        {
          method: 'POST',
          body: JSON.stringify({ subscriptionTopic: topicPattern }),
          headers: {
            accept: 'application/json',
            'content-type': 'application/json',
            encoding: 'utf-8',
            authorization: 'Basic ' + this.options.token
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

  async deleteSubscription(topicPattern, queueName = this.queueName) {
    this.LOG._info && this.LOG.info('Delete subscription', { topic: topicPattern, queue: queueName })
    try {
      await fetch(
        this.options.uri +
          `/SEMP/v2/config/msgVpns/${this.options.vpn}/queues/${encodeURIComponent(queueName)}/subscriptions/${encodeURIComponent(topicPattern)}`,
        {
          method: 'DELETE',
          headers: {
            accept: 'application/json',
            authorization: 'Basic ' + this.options.token
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

  async createQueueAndSubscriptions() {
    this.LOG._info && this.LOG.info(`Create messaging artifacts`)

    const created = await this.createQueue()
    if (!created) {
      // We need to make sure to only keep our own subscriptions
      const resGet = await this.getSubscriptions()
      if (Array.isArray(resGet)) {
        const existingSubscriptions = resGet.map(s => s.subscriptionTopic)
        const obsoleteSubs = existingSubscriptions.filter(s => !this.subscribedTopics.has(s))
        const additionalSubs = [...this.subscribedTopics]
          .map(kv => kv[0])
          .filter(s => !existingSubscriptions.some(e => s === e))
        const unchangedSubs = []
        // eslint-disable-next-line no-unused-vars
        for (const [s, _] of this.subscribedTopics) {
          if (existingSubscriptions.some(e => s === e)) unchangedSubs.push(s)
        }
        this.LOG._info && this.LOG.info('Unchanged subscriptions', unchangedSubs)
        await Promise.all([
          ...obsoleteSubs.map(s => this.deleteSubscription(s)),
          ...additionalSubs.map(async t => this.createSubscription(t))
        ])
        return
      }
    }
    await Promise.all([...this.subscribedTopics].map(kv => kv[0]).map(t => this.createSubscription(t)))
  }

  async deploy() {
    await this.createQueueAndSubscriptions()
  }

  async undeploy() {
    this.LOG._info && this.LOG.info(`Delete messaging artifacts`)
    await this.deleteQueue()
  }
}

class Client {
  constructor(opts) {
    this.options = opts
  }

  async connect() {
    //this.client = new ClientAmqp(this.optionsAMQP)
    //this.sender = sender(this.client, this.service.optionsApp)
    //this.stream = this.sender.attach('')
    //await connect(this.client, this.service.LOG, this.keepAlive)

    return new Promise((resolve, reject) => {
      const factoryProps = new solace.SolclientFactoryProperties();
      factoryProps.profile = solace.SolclientFactoryProfiles.version10;
      solace.SolclientFactory.init(factoryProps);
      this.session = solace.SolclientFactory.createSession({
        url: this.options.uri,
        vpnName: this.options.vpn,
        userName: this.options.user,
        password: this.options.password,
        connectRetries: -1,
      });
      try {
        this.session.connect();
      } catch (error) {
        reject(error)
      } 
      this.session.on(solace.SessionEventCode.UP_NOTICE, () => resolve())
      this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, e => reject(e))
    })
  }

  async disconnect() {
    if (this.session) this.session.disconnect()
  }

  async emit(msg) {
    if (!this.session) await this.connect()

    const { data, event: topic, headers = {} } = msg
    const _message = { ...headers, data }

    const contentType = ['id', 'source', 'specversion', 'type'].every(el => el in headers)
        ? 'application/cloudevents+json'
        : 'application/json'


    const message = solace.SolclientFactory.createMessage()
    message.setDestination(solace.SolclientFactory.createTopicDestination(topic))
    message.setBinaryAttachment(JSON.stringify(_message))
    message.setHttpContentType(contentType)
    message.setDeliveryMode(solace.MessageDeliveryModeType.DIRECT);
    this.session.send(message); // sync???!


    //// REVISIT: Is this a robust way to find out if the connection is working?
    //if (msg._fromOutbox && !this.sender.opened()) throw new Error('AMQP: Sender is not open')
    //await emit(msg, this.stream, this.prefix.topic, this.service.LOG)
    //if (!this.keepAlive) return this.disconnect()
  }

  async listen(cb) {
    if (!this.session) await this.connect()
    const consumer = this.session.createMessageConsumer({
      // solace.MessageConsumerProperties
      queueDescriptor: { name: this.options.queue, type: solace.QueueType.QUEUE },
      acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT,
    });
    consumer.connect()
    this.session.on(solace.SessionEventCode.MESSAGE, async function(message) {
      console.log('>>>>>> tech message received', message)
      const payload = message.getBinaryAttachment()
      const topic = message.getDestination().getName()
      await cb(topic, payload.toString(), null, { done: message.acknowledge, failed: () => message.settle(solace.MessageOutcome.REJECTED) })
    })

    //return addDataListener(this.client, this.service.queueName, this.prefix.queue, cb)
  }
}

module.exports = class AdvancedEventMesh extends EnterpriseMessagingShared {

  getClient() {
    // AMQP -----------------
    // not needed with cds >= 8.7.0
    //if (this.client) return this.client
    //const AMQPClient = require('@sap/cds/libx/_runtime/messaging/common-utils/AMQPClient')
    //this.client = new AMQPClient(this.getClientOptions())
    //return this.client
    // AMQP -----------------

    this.client = new Client(this.getClientOptions())
    return this.client
  }

  getClientOptions() {
    const credentials = this.options.credentials
    if (!credentials) throw new Error(requiredParams)
    return Object.assign({ queue: this.queueName }, credentials)
  }

  getManagement() {
    if (this.management) return this.management
    const optsManagement = this.optionsManagement()
    const queueConfig = this.queueConfig
    const queueName = this.queueName
    this.management = new AEMManagement({
      optionsManagement: optsManagement,
      queueConfig,
      queueName,
      subscribedTopics: this.subscribedTopics,
      LOG: this.LOG
    })
    return this.management
  }

  optionsManagement() {
    const management = this.options.credentials?.management
    if (!management.uri || !management.user || !management.password) {
      throw new Error(requiredParams)
    }
    // TODO: real management APIs
    const creds = {
      uri: management.uri,
      token: Buffer.from(management.user + ':' + management.password).toString('base64'),
      vpn: this.options.credentials.vpn,
      owner: this.options.credentials.user
    }
    return creds
  }
}
