const cds = require('@sap/cds')
cds.test.in(__dirname)

const DATA = { key1: 1, value1: 1 }
const MUST_FAIL = { mustFail: true, value1: 1 }
const MUST_REJECT = { mustReject: true, value1: 1 }
const DATA2 = { key2: 2, value2: 2 }
const HEADERS = { keyHeader1: 1, valueHeader1: 1 }
const HEADERS2 = { keyHeader2: 2, valueHeader2: 2 }

let messaging

const check = {
  sentMessages: []
}

jest.mock('solclientjs', () => {
  return {
    SolclientFactory: {
      createSession(opts) {
        expect(opts.url).toBe('wss://foobar.messaging.solace.cloud:443')
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
          }
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
    }
  }
})

global.fetch = jest.fn((url, opts) => {
  if (!opts.method && url.match(/\/subscriptions$/)) {
    return Promise.resolve({
      json: () => Promise.resolve({ data: [{ subscriptionTopic: 'toBeDeleted' }] })
    })
  }
  return Promise.resolve({
    status: 200,
    json: () => Promise.resolve('default response')
  })
})

jest.mock('https', () => {
  const { Readable } = require('stream')
  const noop = () => {}
  return {
    Agent: class {},
    request: (url, opts, cb) => {
      const res = new Readable()
      res.push(JSON.stringify({ access_token: '<sampleToken>', expires_in: 1 }))
      res.push(null)
      Object.assign(res, { headers: { 'content-type': 'application/json' } })
      setTimeout(() => cb(res), 1)
      return { on: noop, write: noop, end: noop }
    }
  }
})

describe('simple unit tests', () => {
  cds.test()

  beforeAll(async () => {
    messaging = await cds.connect.to('messaging')
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

  test('failed consumption because of no handler', done => {
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
        try {
          expect(e).toBe(1)
          done()
        } catch (e) {
          done(e)
        }
      }
    })
  })

  test('failed consumption because of failure', done => {
    messaging.messageConsumer.emit('MESSAGE', {
      getDestination() {
        return {
          getName() {
            return 'cap.external.object.changed.v1'
          }
        }
      },
      getBinaryAttachment() {
        return JSON.stringify({ data: MUST_FAIL, ...HEADERS })
      },
      async acknowledge() {
        done(new Error('Should not have succeeded'))
      },
      settle(e) {
        try {
          expect(e).toBe(1)
          done()
        } catch (e) {
          done(e)
        }
      }
    })
  })

  test('failed consumption because of reject', done => {
    messaging.messageConsumer.emit('MESSAGE', {
      getDestination() {
        return {
          getName() {
            return 'cap.external.object.changed.v1'
          }
        }
      },
      getBinaryAttachment() {
        return JSON.stringify({ data: MUST_REJECT, ...HEADERS })
      },
      async acknowledge() {
        done(new Error('Should not have succeeded'))
      },
      settle(e) {
        try {
          expect(e).toBe(3)
          done()
        } catch (e) {
          done(e)
        }
      }
    })
  })

  test('fresh new token', done => {
    setTimeout(() => {
      expect(messaging.session.updateAuthenticationOnReconnect).toHaveBeenCalled()
      done()
    }, 1000)
  })
})
