const cds = require('@sap/cds')

cds.test(__dirname, '--profile', 'solaceProps')

const { mockMessageAcc } = require('../mocks/solclientjs')
jest.mock('solclientjs', () => require('../mocks/solclientjs'))
jest.mock('https', () => require('../mocks/https'))
global.fetch = require('../mocks/fetch')

const DATA = { key1: 1, value1: 1 }

let messaging

describe('when msgHeadersAsSolaceProps=true to activate forwarding of headers as solace props', () => {
  
  beforeAll(async () => {
    messaging = await cds.connect.to('messaging')
  }, 3000)

  test('should forward known headers as solace props and unknown headers as user props', async () => {
    const before = mockMessageAcc.sentMessages.length
    
    const headers = {
      // string properties
      applicationMessageId:   'msg-id-1',
      applicationMessageType: 'my-type',
      correlationId:          'corr-1',
      httpContentEncoding:    'gzip',
      httpContentType:        'application/json',
      senderId:               'sender-1',
      userData:               'ud',

      // number properties
      gmExpiration:    1000,
      priority:        4,
      senderTimestamp: 1700000000000,
      sequenceNumber:  42,
      timeToLive:      5000,
      
      // boolean properties
      acknowledgeImmediately: true,
      asReplyMessage:         false,
      deliverToOne:           true,
      dmqEligible:            false,
      elidingEligible:        true,

      customFlag: true,
      customText: 'x',
      customNum: 7.0
    }

    await messaging.emit('baz', DATA, headers)
    const sent = mockMessageAcc.sentMessages[before]

    // known properties set via their adequate setters
    expect(sent.applicationMessageId).toBe('msg-id-1')
    expect(sent.applicationMessageType).toBe('my-type')
    expect(sent.correlationId).toBe('corr-1')
    expect(sent.httpContentEncoding).toBe('gzip')
    expect(sent.httpContentType).toBe('application/json')
    expect(sent.senderId).toBe('sender-1')
    expect(sent.userData).toBe('ud')
    expect(sent.gmExpiration).toBe(1000)
    expect(sent.priority).toBe(4)
    expect(sent.senderTimestamp).toBe(1700000000000)
    expect(sent.sequenceNumber).toBe(42)
    expect(sent.timeToLive).toBe(5000)
    expect(sent.acknowledgeImmediately).toBe(true)
    expect(sent.asReplyMessage).toBe(false)
    expect(sent.deliverToOne).toBe(true)
    expect(sent.dmqEligible).toBe(false)
    expect(sent.elidingEligible).toBe(true)

    // remaining headers routed into the user property map, typed
    const map = sent.userPropertyMap
    expect(map.getKeys().sort()).toEqual(['customFlag', 'customNum', 'customText'])
    expect(map.getField('customFlag')).toMatchObject({ type: 0, value: true })
    expect(map.getField('customNum')).toMatchObject({ type: 8, value: 7 })
    expect(map.getField('customText')).toMatchObject({ type: 10, value: 'x' })

    // headers are also included 'binary attachment'
    expect(sent.binary).toBe(JSON.stringify({ data: DATA, ...headers }))
  })
})
