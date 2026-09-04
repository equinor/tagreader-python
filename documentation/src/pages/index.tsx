import React from 'react'
import { Redirect } from 'react-router-dom'

export default function Home(): React.JSX.Element {
  return <Redirect to="docs/about/introduction" />
}
