import { useAtom, type PrimitiveAtom } from 'jotai'
import { type ComponentProps } from 'react'
import { Input } from './ui/input'

type AtomInputProps = Omit<ComponentProps<typeof Input>, 'value' | 'onChange'> & {
  atom: PrimitiveAtom<string>
}

/**
 * Self-subscribing input bound to a jotai atom. Use this when typing into
 * the input must NOT re-render the surrounding component — only this wrapper
 * subscribes via useAtom; the parent stays untouched.
 *
 * Read the atom value elsewhere with useAtomValue (reactive) or
 * useStore().get(atom) (non-reactive, for mutation closures).
 */
export function AtomInput({ atom: theAtom, ...rest }: AtomInputProps) {
  const [value, setValue] = useAtom(theAtom)
  return <Input value={value} onChange={(event) => setValue(event.target.value)} {...rest} />
}
