import React from 'react'
import Header from './Header'
import Form from './Form'
import Footer from './Footer'

export default () => {
  return (
    <div className="app">
      <Header title="Artivest QE" subtitle="Sign up for our newsletter." />
      <Form />
      <Footer />
    </div>
  )
}
