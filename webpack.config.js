var config = {
  entry: './static/app.jsx',

  output: {
    path: __dirname + '/dist',
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
