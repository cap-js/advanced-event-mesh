const cds = require('@sap/cds')
const {
  SolclientFactory,
  SolclientFactoryProperties,
  SessionEventCode,
  MessageConsumerEventName
} = require('solclientjs')
cds.test.in(__dirname)
const DATA = { key1: 1, value1: 1 }
const DATA2 = { key2: 2, value2: 2 }
const HEADERS = { keyHeader1: 1, valueHeader1: 1 }
const HEADERS2 = { keyHeader2: 2, valueHeader2: 2 }
let messaging, credentials

const check = {
  sentMessages: []
}

jest.mock('solclientjs', () => {
  return {
    SolclientFactory: {
      createSession(opts) {
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
        s.createMessageConsumer = queue => {
          return c
        }
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
          }
        }
      },
      createTopicDestination(topic) {
        return topic
      },
      init(opts) {},
      setLogLevel(opts) {}
    },
    MessageConsumerEventName: {
      MESSAGE: 'MESSAGE',
      UP: 'UP'
    },
    MessageDeliveryModeType: {
      PERSISTENT: 'PERSISTENT'
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
      FAILED: 'FAILED'
    }
  }
})

global.fetch = jest.fn((url, opts) => {
  console.log('url:', url)
  if (url === '<tokenendpoint>') {
    return Promise.resolve({
      json: () => Promise.resolve('<sampleToken>')
    })
  } else if (url === '<management-uri>/SEMP/v2/config/msgVpns/<vpn>/queues/CAP%2F0000/subscriptions') {
    return Promise.resolve({
      json: () => Promise.resolve({ data: [{ subscriptionTopic: 'toBeDeleted' }] })
    })
  }
  return Promise.resolve({
    json: () => Promise.resolve('default response')
  })
})

describe('simple unit tests', () => {
  cds.test()

  beforeAll(async () => {
    messaging = await cds.connect.to('messaging')
    credentials = messaging.options.credentials
  })

  test('emit from app service', async () => {
    await messaging.emit('foo', DATA, HEADERS)
    await messaging.emit('bar', DATA2, HEADERS2)
    expect(check.sentMessages[0].binary).toBe(JSON.stringify({ data: DATA, ...HEADERS }))
    expect(check.sentMessages[0].dest).toBe('foo')
    expect(check.sentMessages[0].mode).toBe('PERSISTENT')
    expect(check.sentMessages[1].binary).toBe(JSON.stringify({ data: DATA2, ...HEADERS2 }))
    expect(check.sentMessages[1].dest).toBe('bar')
    expect(check.sentMessages[1].mode).toBe('PERSISTENT')
  })

  test('successful consumption', done => {
    messaging.messageConsumer.emit('MESSAGE', {
      getDestination() {
        return {
          getName() {
            return 'cap.external.object.changed.v1'
          }
        }
      },
      getBinaryAttachment() {
        return JSON.stringify({ data: DATA, ...HEADERS })
      },
      async acknowledge() {
        const messages = await SELECT.from('db.Messages')
        try {
          expect(messages[0].event).toBe('changed')
          expect(messages[0].data).toBe(JSON.stringify(DATA))
          expect(messages[0].headers).toBe(JSON.stringify(HEADERS))
          done()
        } catch (e) {
          done(e)
        }
      },
      settle() {
        done(new Error('Message could not be received'))
      }
    })
  })

  test('failed consumption', done => {
    messaging.messageConsumer.emit('MESSAGE', {
      getDestination() {
        return {
          getName() {
            return 'does_not_have_a_handler'
          }
        }
      },
      getBinaryAttachment() {
        return JSON.stringify({ data: DATA, ...HEADERS })
      },
      async acknowledge() {
        done(new Error('Should not have succeeded'))
      },
      settle(e) {
        console.log({ e })
        done()
      }
    })
  })
})
