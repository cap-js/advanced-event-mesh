const cds = require('@sap/cds')
const { SolclientFactory, SolclientFactoryProperties, SessionEventCode } = require('solclientjs')
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
        s.connect = jest.fn(() => {
          s.emit('UP_NOTICE')
        })
        s.send = (msg) => {
          c.emit('MESSGE', msg) // TODO
          s.emit('ACKNOWLEDGED_MESSAGE', msg)
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
  if (url === '<tokenendpoint>') {
    return Promise.resolve({
      json: () => Promise.resolve('<sampleToken>'),
    });
  }
  return Promise.resolve({
    json: () => Promise.resolve('default response'),
  });
});

//jest.mock('@sap/xssec', () => ({
//  createSecurityContext(token, _credentials, id, cb) {
//    if (token !== 'dummyToken') return cb(null, null, null)
//    const dummyContext = {}
//    const tokenInfoObj = { sub: 'eb-client-id', azp: 'eb-client-id' }
//    const dummyTokenInfo = {
//      getPayload: () => tokenInfoObj,
//      getClientId: () => 'eb-client-id',
//      getZoneId: () => 'dummyZoneId',
//      ...tokenInfoObj
//    }
//    return cb(null, dummyContext, dummyTokenInfo)
//  }
//}))

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
    await messaging.emit('foo', DATA, HEADERS)
    //mockHttps.handleHttpReq = () => {
    //  return { message: 'ok' }
    //}
    //cds.context = { tenant: 't1', user: cds.User.privileged }
    //try {
    //  await ownSrv.emit('created', { data: 'testdata', headers: { some: 'headers' } })
    //  expect(1).toBe('Should not be supported')
    //} catch (e) {
    //  expect(e.message).toMatch(/not supported/)
    //}
  })
})

