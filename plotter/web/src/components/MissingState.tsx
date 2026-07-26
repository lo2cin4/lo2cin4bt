type Props = {
  message: string
}

export function MissingState({ message }: Props) {
  return <div className="missing-state">{message}</div>
}
