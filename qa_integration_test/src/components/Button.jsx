import React from 'react'

export default ({ type, value, onClick }) => {
  return (
    <button className="button" type={ type } onClick={ onClick }>
      { value }
    </button>
  )
}
