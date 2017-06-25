var config = {
  entry: './static/js/app.jsx',

  output: {
    path: __dirname + '/dist/js',
    filename: 'app.js',
  },

  resolve: {
    extensions: [".jsx", ".json", ".js"]
  },

  module: {
    loaders: [
      {
        loader: 'babel-loader',
        test: /\.jsx?$/,
        exclude: /node_modules/,
        query: {
          presets: ['es2015', 'react']
        }
      }
    ]
  }
}

module.exports = config;
