import typescript from 'rollup-plugin-typescript2';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default {
  input: 'src/index.ts',
  output: {
    file: 'lib/index.js',
    format: 'esm',
    sourcemap: true
  },
  external: [
    '@jupyterlab/application',
    '@jupyterlab/codemirror',
    '@codemirror/language',
    '@lezer/highlight'
  ],
  plugins: [resolve(), commonjs(), typescript({ tsconfig: './tsconfig.json' })]
};
