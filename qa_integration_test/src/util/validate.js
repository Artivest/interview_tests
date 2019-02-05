export default form => {
  const regex = /^[a-z0-9]+@[a-z0-9\.]+$/i
  let emailValid = null

  const nameValid = (form.name === '') ? 'Please enter your full name.' : null

  if (form.email === '') {
    emailValid = 'Please enter your email.'
  }
  if (!regex.test(form.email)) {
    emailValid = 'Please enter a valid email'
  } 

  return {
    name: nameValid,
    email: emailValid
  }
}
