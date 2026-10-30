/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'norse': {
          'night': '#0a0e1a',
          'shadow': '#141824',
          'stone': '#1e2433',
          'rune': '#2a3247',
          'fog': '#3d4659',
          'silver': '#9ca3af',
        },
        'valhalla': {
          'gold': '#d4af37',
          'amber': '#e8b84a',
          'bronze': '#cd7f32',
        },
        'frost': {
          'ice': '#4a9eff',
          'glacier': '#7dd3fc',
        },
        'nornic': {
          'primary': '#10b981',
          'secondary': '#059669',
          'accent': '#34d399',
        },
      },
      fontFamily: {
        'mono': ['JetBrains Mono', 'Fira Code', 'monospace'],
        'display': ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
