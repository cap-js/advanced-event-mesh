const cds = require('@sap/cds')
cds.test.in(__dirname)
const DATA = { key1: 1, value1: 1 }
const HEADERS = { keyHeader1: 1, valueHeader1: 1 }
let messaging, credentials

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

