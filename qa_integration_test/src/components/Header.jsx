import React from 'react'

export default ({ title, subtitle }) => {
  const imgUrl = 'https://crunchbase-production-res.cloudinary.com/image/upload/c_lpad,h_256,w_256,f_auto,q_auto:eco/v1405452555/nfqkd2f6t8bc5uc8du6g.png';

  return (
    <div className="header">

      <div className="header__logo">
        <img className="header__img" src={ imgUrl } />
      </div>

      <h1 className="header__title">{ title }</h1>
      <h2 className="header__subtitle">{ subtitle }</h2>

    </div>
  )
}
