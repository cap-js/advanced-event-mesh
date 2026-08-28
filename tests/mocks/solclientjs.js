// solclientjs fake for the `simple` test app. Wired via
// `jest.mock('solclientjs', () => require('../mocks/solclientjs'))`.
//
// Also exports `check` — the assertion sink the mock pushes sent messages to.
// jest gives each test file a fresh module registry, so each file gets its own
// `check` instance (sentMessages do not bleed across files), and within a file
// the mock and the test resolve the same singleton.

const check = {
  sentMessages: []
}

module.exports = {
  check,
  SolclientFactory: {
    createSession(opts) {
      expect(opts.url).toBe('wss://foobar.messaging.solace.cloud:456')
      expect(opts.vpnName).toBe('<vpn>')
      expect(opts.accessToken).toBe('<sampleToken>')
      expect(opts.authenticationScheme).toBe('AuthenticationScheme_oauth2')
      expect(opts.customSessionOpt).toBe(true)
      const EventEmitter = require('events')
      const s = new EventEmitter()
      const c = new EventEmitter()
      s.connect = () => {
        s.emit('UP_NOTICE')
      }
      s.send = msg => {
        c.emit('MESSGE', msg)
        check.sentMessages.push(msg)
        s.emit('ACKNOWLEDGED_MESSAGE', msg)
      }
      s.createMessageConsumer = opts => {
        expect(opts.customConsumerOpt).toBe(true)
        return c
      }
      s.updateAuthenticationOnReconnect = jest.fn(opts => {
        expect(opts.accessToken).toBeDefined()
      })
      c.connect = () => {
        c.emit('UP')
      }

      return s
    },
    createMessage() {
      return {
        setDestination(dest) {
          this.dest = dest
        },
        setBinaryAttachment(binary) {
          this.binary = binary
        },
        setDeliveryMode(mode) {
          this.mode = mode
        },
        setCorrelationKey(corr) {
          this.correlationKey = corr
        },
        setCorrelationId(v) { this.correlationId = v },
        setApplicationMessageId(v) { this.applicationMessageId = v },
        setApplicationMessageType(v) { this.applicationMessageType = v },
        setHttpContentEncoding(v) { this.httpContentEncoding = v },
        setHttpContentType(v) { this.httpContentType = v },
        setSenderId(v) { this.senderId = v },
        setUserData(v) { this.userData = v },
        setGMExpiration(v) { this.gmExpiration = v },
        setSenderTimestamp(v) { this.senderTimestamp = v },
        setSequenceNumber(v) { this.sequenceNumber = v },
        setTimeToLive(v) { this.timeToLive = v },
        setPriority(v) { this.priority = v },
        setAcknowledgeImmediately(v) { this.acknowledgeImmediately = v },
        setAsReplyMessage(v) { this.asReplyMessage = v },
        setDeliverToOne(v) { this.deliverToOne = v },
        setDMQEligible(v) { this.dmqEligible = v },
        setElidingEligible(v) { this.elidingEligible = v },
        setUserPropertyMap(v) { this.userPropertyMap = v }
      }
    },
    createTopicDestination(topic) {
      return topic
    },
    init() {},
    setLogLevel(lvl) {
      expect(lvl).toBe(666)
    }
  },
  MessageConsumerEventName: {
    MESSAGE: 'MESSAGE',
    UP: 'UP'
  },
  MessageDeliveryModeType: {
    PERSISTENT: 'PERSISTENT'
  },
  MessageType: {
    BINARY: 0,
    MAP: 1,
    STREAM: 2,
    TEXT: 3
  },
  SolclientFactoryProperties: class {},
  SolclientFactoryProfiles: {},
  SessionEventCode: {
    UP_NOTICE: 'UP_NOTICE',
    CONNECT_FAILED_ERROR: 'CONNECT_FAILED_ERROR',
    ACKNOWLEDGED_MESSAGE: 'ACKNOWLEDGED_MESSAGE',
    REJECTED_MESSAGE_ERROR: 'REJECTED_MESSAGE_ERROR'
  },
  MessageOutcome: {
    FAILED: 1,
    REJECTED: 3
  },
  SDTFieldType: {
    BOOL: 0,
    INT64: 8,
    DOUBLETYPE: 13,
    STRING: 10
  },
  SDTMapContainer: class {
    constructor() { this._fields = {} }
    addField(key, type, value) { this._fields[key] = { type, value } }
    getField(key) { return this._fields[key] }
    getKeys() { return Object.keys(this._fields) }
  }
}
