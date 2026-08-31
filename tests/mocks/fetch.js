// global.fetch fake for SEMP management calls. Wired via
// `global.fetch = require('../mocks/fetch')`.

module.exports = jest.fn((url, opts) => {
  if (!opts.method && url.match(/\/subscriptions$/)) {
    expect(url).toMatch(/^https:\/\/[\w.]+:123\/SEMP\/v2\/config\/.+$/)
    return Promise.resolve({
      json: () => Promise.resolve({ data: [{ subscriptionTopic: 'toBeDeleted' }] })
    })
  }
  return Promise.resolve({
    status: 200,
    json: () => Promise.resolve('default response')
  })
})
