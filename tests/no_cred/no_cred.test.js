const AEM = require('../../cds-plugin')

describe('No Credentials', () => {
  test('no binding at all', async () => {
    const aem = new AEM()
    try {
      await aem.init()
      expect(1).toBe(2)
    } catch (e) {
      expect(e.message).toMatch('Missing or malformed credentials')
    }
  })

  test('not enough binding info', async () => {
    const aem = new AEM()
    aem.credentials = { something: 'unrelated' }
    try {
      await aem.init()
      expect(1).toBe(2)
    } catch (e) {
      expect(e.message).toMatch('Missing or malformed credentials')
    }
  })
})
