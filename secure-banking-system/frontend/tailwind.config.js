/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'bank-navy': '#0f172a',
        'bank-blue': '#3b82f6',
        'cyborg-green': '#10b981',
      }
    },
  },
  plugins: [],
}