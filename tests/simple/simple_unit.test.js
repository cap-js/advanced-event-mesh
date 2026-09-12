const cds = require('@sap/cds')
cds.test.in(__dirname)

const DATA = { key1: 1, value1: 1 }
const MUST_FAIL = { mustFail: true, value1: 1 }
const MUST_REJECT = { mustReject: true, value1: 1 }
const DATA2 = { key2: 2, value2: 2 }
const HEADERS = { keyHeader1: 1, valueHeader1: 1 }
const HEADERS2 = { keyHeader2: 2, valueHeader2: 2 }

let messaging

const { mockMessageAcc } = require('../mocks/solclientjs')
jest.mock('solclientjs', () => require('../mocks/solclientjs'))
jest.mock('https', () => require('../mocks/https'))
global.fetch = require('../mocks/fetch')

describe('simple unit tests', () => {
  cds.test()

  beforeAll(async () => {
    messaging = await cds.connect.to('messaging')
  }, 30000)

  test('emit from app service', async () => {
    await messaging.emit('foo', DATA, HEADERS)
    await messaging.emit('bar', DATA2, HEADERS2)
    expect(mockMessageAcc.sentMessages[0].binary).toBe(JSON.stringify({ data: DATA, ...HEADERS }))
    expect(mockMessageAcc.sentMessages[0].dest).toBe('foo')
    expect(mockMessageAcc.sentMessages[0].mode).toBe('PERSISTENT')
    expect(mockMessageAcc.sentMessages[1].binary).toBe(JSON.stringify({ data: DATA2, ...HEADERS2 }))
    expect(mockMessageAcc.sentMessages[1].dest).toBe('bar')
    expect(mockMessageAcc.sentMessages[1].mode).toBe('PERSISTENT')
  })

  test('should not set solace properties from headers or custom headers when msgHeadersAsSolaceProps=false', async () => {

    const before = mockMessageAcc.sentMessages.length
    const headers = { correlationId: 'corr-2', priority: 9, customText: 'y' }

    await messaging.emit('qux', DATA, headers)
    const sent = mockMessageAcc.sentMessages[before]

    expect(sent.correlationId).toBeUndefined()
    expect(sent.priority).toBeUndefined()
    expect(sent.userPropertyMap).toBeUndefined()
    expect(sent.binary).toBe(JSON.stringify({ data: DATA, ...headers }))
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
      getType() {
        return 0 //> not TEXT (=== 3)
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
      getType() {
        return 0 //> not TEXT (=== 3)
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
      getType() {
        return 0 //> not TEXT (=== 3)
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
      getType() {
        return 0 //> not TEXT (=== 3)
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

  test('malformed payload "null" is settled, not leaked', done => {
    messaging.messageConsumer.emit('MESSAGE', {
      getDestination() {
        return {
          getName() {
            return 'cap.external.object.changed.v1'
          }
        }
      },
      getType() {
        return 0 //> not TEXT (=== 3)
      },
      getBinaryAttachment() {
        return 'null'
      },
      async acknowledge() {
        done(new Error('Should not have acknowledged: malformed payload must settle'))
      },
      settle(e) {
        try {
          expect(e).toBe(1) //> FAILED — handler couldn't process `null` payload
          done()
        } catch (err) {
          done(err)
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

  test('listening', () => {
    messaging.on('cap.external.object.changed.v1', () => {})
    cds.emit('listening')
    expect(fetch).toHaveBeenCalledWith('<handshake uri>', {
      body: '{"hostName":"foobar.messaging.solace.cloud","subaccountId":"foo bar"}',
      headers: { Authorization: 'Bearer <sampleToken>' },
      method: 'POST'
    })
    expect(fetch).toHaveBeenCalledWith(
      'https://foobar.messaging.solace.cloud:123/SEMP/v2/config/msgVpns/<vpn>/queues',
      {
        method: 'POST',
        body: '{"permission":"consume","ingressEnabled":true,"egressEnabled":true,"customQueueOpt":true,"queueName":"testQueueName"}',
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          encoding: 'utf-8',
          authorization: 'Bearer <sampleToken>'
        }
      }
    )
    expect(fetch).toHaveBeenCalledWith(
      'https://foobar.messaging.solace.cloud:123/SEMP/v2/config/msgVpns/<vpn>/queues/testQueueName/subscriptions',
      { headers: { accept: 'application/json', authorization: 'Bearer <sampleToken>' } }
    )
  })

  test('skipManagement listening', async () => {
    const opts = Object.assign({}, messaging.options)
    opts.skipManagement = true
    opts.queue.name = 'testQueueName2'
    const messagingSkipped = await cds.connect.to('messagingSkipped', opts)
    messagingSkipped.on('cap.external.object.changed.v1', () => {})
    cds.emit('listening')
    expect(fetch).not.toHaveBeenCalledWith(
      'https://foobar.messaging.solace.cloud:123/SEMP/v2/config/msgVpns/<vpn>/queues',
      {
        method: 'POST',
        body: '{"permission":"consume","ingressEnabled":true,"egressEnabled":true,"customQueueOpt":true,"queueName":"testQueueName2"}',
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          encoding: 'utf-8',
          authorization: 'Bearer <sampleToken>'
        }
      }
    )
    expect(fetch).not.toHaveBeenCalledWith(
      'https://foobar.messaging.solace.cloud:123/SEMP/v2/config/msgVpns/<vpn>/queues/testQueueName2/subscriptions',
      { headers: { accept: 'application/json', authorization: 'Bearer <sampleToken>' } }
    )
  })
})
