import React from 'react'

const date = new Date()

export default () => {
  return (
    <div className="footer">
      &copy; { date.getFullYear() } Artivest
    </div>
  )
}
