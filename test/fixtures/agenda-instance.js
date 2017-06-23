var connStr = process.argv[2];
var tests = process.argv.slice(3);

var path = require('path'),
    Agenda = require( path.join('..', '..', 'node_modules', 'agenda', 'lib', 'agenda.js') ),
    addTests = require( path.join(__dirname, 'addTests.js') );

var agenda = new Agenda({
  adapter: require('../../index.js'),
  db: {
    credentials: {
      connectionString: connStr,
      dialectOptions: {
        logging: false,
      },
    },
    collection: "agenda-test"
  }
}, function(err, collection) {

	tests.forEach(function(test) {
	  addTests[test](agenda);
	});

	agenda.start();

	// Ensure we can shut down the process from tests
	process.on('message', function(msg) {
	  if( msg == 'exit' ) process.exit(0);
	});

	// Send default message of "notRan" after 400ms
	setTimeout(function() {
	  process.send('notRan');
	  process.exit(0);
	}, 400);

});

