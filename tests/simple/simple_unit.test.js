const cds = require('@sap/cds')
const { SolclientFactory, SolclientFactoryProperties, SessionEventCode, MessageConsumerEventName } = require('solclientjs')
cds.test.in(__dirname)
const DATA = { key1: 1, value1: 1 }
const HEADERS = { keyHeader1: 1, valueHeader1: 1 }
let messaging, credentials


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
        s.send = (msg) => {
          c.emit('MESSGE', msg) // TODO
          s.emit('ACKNOWLEDGED_MESSAGE', msg)
        }
        s.createMessageConsumer = (queue) => {
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
          },
          setBinaryAttachment(binary) {
          },
          setDeliveryMode() {
          },
          setCorrelationKey(corr) {
            this.correlationKey = corr
          }
        }
      },
      createTopicDestination(topic) {
        return topic
      },
      init(opts) {
      },
      setLogLevel(opts) {
      }
    },
    MessageConsumerEventName: {
      MESSAGE: 'MESSAGE',
      UP: 'UP'
    },
    MessageDeliveryModeType: {
      PERSISTENT: 'PERSISTENT'
    },
    SolclientFactoryProperties: class {
    },
    SolclientFactoryProfiles: {},
    SessionEventCode: {
      UP_NOTICE: 'UP_NOTICE',
      CONNECT_FAILED_ERROR: 'CONNECT_FAILED_ERROR',
      ACKNOWLEDGED_MESSAGE: 'ACKNOWLEDGED_MESSAGE',
      REJECTED_MESSAGE_ERROR: 'REJECTED_MESSAGE_ERROR'
    }
  }
})


global.fetch = jest.fn((url, opts) => {
  console.log('url:', url)
  if (url === '<tokenendpoint>') {
    return Promise.resolve({
      json: () => Promise.resolve('<sampleToken>'),
    });
  } else if (url === '<management-uri>/SEMP/v2/config/msgVpns/<vpn>/queues/CAP%2F0000/subscriptions') {
    return Promise.resolve({
      json: () => Promise.resolve({ data: [{subscriptionTopic: 'toBeDeleted'}] }),
    });
  }
  return Promise.resolve({
    json: () => Promise.resolve('default response'),
  });
});

describe('simple unit tests', () => {
  const { POST } = cds.test()

  beforeAll(async () => {
    messaging = await cds.connect.to('messaging')
    credentials = messaging.options.credentials
  })
  beforeEach(() => {
    //mockHttps.request.mockClear()
    //messaging.options.credentials = credentials
  })

  test('emit from app service', async () => {
    messaging.emit('foo', DATA, HEADERS)
  })
})

