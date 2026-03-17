import { useEffect, useRef, useState } from 'react'
import { motion, useSpring, useMotionValue } from 'framer-motion'

interface AnimatedNumberProps {
  value: number
  prefix?: string
  suffix?: string
  decimals?: number
  className?: string
  duration?: number
}

export default function AnimatedNumber({
  value,
  prefix = '',
  suffix = '',
  decimals = 1,
  className = '',
  duration = 0.6,
}: AnimatedNumberProps) {
  const motionValue = useMotionValue(0)
  const springValue = useSpring(motionValue, {
    stiffness: 100,
    damping: 20,
    duration: duration * 1000,
  })
  const [displayValue, setDisplayValue] = useState(value)
  const prevValue = useRef(value)

  useEffect(() => {
    motionValue.set(prevValue.current)
    // Small delay to trigger the spring
    const raf = requestAnimationFrame(() => {
      motionValue.set(value)
    })
    prevValue.current = value
    return () => cancelAnimationFrame(raf)
  }, [value, motionValue])

  useEffect(() => {
    const unsubscribe = springValue.on('change', (latest) => {
      setDisplayValue(latest)
    })
    return unsubscribe
  }, [springValue])

  const formatted = `${prefix}${displayValue.toFixed(decimals)}${suffix}`

  return (
    <span className={className}>
      {formatted}
    </span>
  )
}

/**
 * Financial-grade animated price tick.
 * Green background pulse + fade on uptick, red on downtick.
 * Inspired by Bloomberg/Reuters terminal tick coloring.
 */
export function FlashNumber({
  value,
  prefix = '',
  suffix = '',
  decimals = 2,
  className = '',
}: AnimatedNumberProps & {
  positiveClass?: string
  negativeClass?: string
}) {
  const [flash, setFlash] = useState<'up' | 'down' | null>(null)
  const prevValue = useRef(value)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (value !== prevValue.current) {
      setFlash(value > prevValue.current ? 'up' : 'down')
      prevValue.current = value
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setFlash(null), 1200)
      return () => {
        if (timerRef.current) clearTimeout(timerRef.current)
      }
    }
  }, [value])

  return (
    <motion.span
      className={`${className} tick-flash ${flash === 'up' ? 'tick-up' : flash === 'down' ? 'tick-down' : ''}`}
      animate={
        flash
          ? { scale: [1, 1.04, 1] }
          : {}
      }
      transition={{ duration: 0.2, ease: 'easeOut' }}
    >
      {prefix}{value.toFixed(decimals)}{suffix}
    </motion.span>
  )
}
