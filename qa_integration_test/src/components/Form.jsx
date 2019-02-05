import React from 'react'
import Input from './Input'
import Button from './Button'
import MessageBox from './MessageBox'
import validate from '../util/validate'
import { postData } from '../util/request'

export default class Form extends React.Component {

  constructor(props) {
    super(props)
    this.state = {
      validation: {
        name: null,
        email: null
      },
      submitted: false,
      status: null,
      message: ''
    }
    this.handleSubmit = this.handleSubmit.bind(this)
  }

  handleSubmit(e) {
    e.preventDefault()
    const form = {
      name: this.name.value,
      email: this.email.value
    }

    // validate form
    const validation = validate(form)
    this.setState({ validation })

    // submit
    if (!validation.name && !validation.email) {
      this.submitForm(form)
    } 
  }

  submitForm(data) {
    postData(data).then(response => {
      if (response.ok) {
        this.setState({ 
          status: true,
          message: 'Success! You\'re request has been received.' 
        })
      } else {
        this.setState({ 
          status: false,
          message: 'Something went wrong... Please refresh and try again, or contact support.' 
        })
      }
      return response.json()
    }).then(data => console.log(data))

    this.setState({ submitted: true })
  }

  render() {
    return (
      <div className="row content">
        <div className="col-sm-8 col-sm-offset-2 col-md-6 col-md-offset-3 col-lg-4 col-lg-offset-4">

          { !this.state.submitted && 
            <form className="form" onSubmit={ this.handleSubmit }>
            
              <Input
                id="full-name"
                inputRef={ el => this.name = el }
                type="text"
                label="Full Name"
                placeholder="Enter name"
                error={ this.state.validation.name !== null }
                msg={ this.state.validation.name } />

              <Input
                id="email"
                inputRef={ el => this.email = el }
                type="text"
                label="Email address"
                placeholder="Enter email"
                error={ this.state.validation.email !== null }
                msg={ this.state.validation.email } />

              <Button type="submit" value="Submit" />

            </form>
          }

          { this.state.submitted && <MessageBox status={ this.state.status } message={ this.state.message } /> }

        </div>
      </div>
    )
  }
}
