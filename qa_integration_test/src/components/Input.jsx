import React from 'react'

export default class Input extends React.Component {

  constructor(props) {
    super(props)
    this.state = {
      value: ''
    }
    this.handleChange = this.handleChange.bind(this)
  }

  handleChange(e) {
    this.setState({
      value: e.target.value
    })
  }

  render() {
    return (
      <div className={ this.props.error ? 'has-error' : '' }>
        <div className="form-group">
          <label htmlFor={ this.props.id }>{ this.props.label }</label>

          <input 
            className="form-control" 
            type={ this.props.type } 
            ref={ this.props.inputRef }
            id={ this.props.id } 
            name={ this.props.id }
            placeholder={ this.props.placeholder }
            value={ this.state.value }
            onChange={ this.handleChange } />

          { this.props.error && <span className="form__error">{ this.props.msg }</span> }
        </div>
      </div>
    )
  }
}
