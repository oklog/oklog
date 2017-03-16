; (function() {
	var app = Elm.Main.fullscreen(),
		left = "",
		stream = null;

	var read = function() {
		var decoder = new TextDecoder();

		stream.read().then(function(result) {
			if (!result.done) {
				var raw = left.concat(decoder.decode(result.value, {stream: true})),
					lines = raw.split("\n");

				left = lines.pop();

				app.ports.streamLine.send(lines);

				return
			}

			app.ports.streamComplete.send("");

			left = "";
			stream = null;
		});
	};

	app.ports.streamContinue.subscribe(function() {
		if (!stream) {
			app.ports.streamError.send("Stream already in progress");
			return
		}

		read();
	});

	app.ports.streamRequest.subscribe(function(url) {
		console.log(url);
		if (stream) {
			app.ports.streamError.send("Stream already in progress");
			return
		}

		fetch(url, {
			method: "GET"
		}).then(function(response) {
			stream = response.body.getReader();

			read();
		});
	});
}).call(this);
