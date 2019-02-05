import React from 'react'

export default ({ status, message }) => {
  return (
    <div className={ 'msg-box ' + (status ? 'msg-box--success' : 'msg-box--error') }>
      <p className="msg-box__msg">{ message }</p>
    </div>
  )
}
