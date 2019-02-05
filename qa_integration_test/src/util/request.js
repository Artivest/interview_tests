const url = 'https://reqres.in/api/register'

// POST
export const postData = (data) => {
  // transform data for test api
  const testData = Object.assign({}, data, { username: data.name, password: 'abc' })

  return fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    method: 'POST',
    body: JSON.stringify(testData)
    }
  )
}
