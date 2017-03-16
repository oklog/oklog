var _oklog$oklog$Native_RFC3339 = function() {
  var partialTime = (
      /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?/
  );

  var timeNumOffset = (
    /^Z|([+-]\d{2}):(\d{2})$/
  );

  var empty = new window.Date(window.Date.UTC(0, 0, 1, 0, 0, 0, 0));  // 1900AD
  empty.setUTCFullYear(0);  // 0AD

  return {
    decode: function(s) {
      var m = partialTime.exec(s);
      if (!m) {
        return Result.Err("unable to parse input as RFC3339");
      }

      var year     = +m[1];
      var month    = +m[2];
      var day      = +m[3];
      var hour     = +m[4];
      var minute   = +m[5];
      var second   = +m[6];
      var msec     = +m[7] || 0;

      s = s.substr(m[0].length);
      m = timeNumOffset.exec(s);
      if (!m) {
        return Result.Err("unable to parse input as RFC3339");
      }

      var tzHour = +m[1] || 0;
      var tzMin  = +m[2] || 0;

      var tzOffset = new window.Date().getTimezoneOffset() + tzHour * 60 + tzMin;
      return Result.Ok(
        new window.Date(year, month - 1, day, hour, minute - tzOffset, second, msec)
      );
    },

    encode: function(d) {
      function pad(pad, n) {
        var str = "" + n;
        return pad.substring(0, pad.length - str.length) + str;
      }

      return pad("0000", d.getUTCFullYear()) + "-"
         +  pad("00", d.getUTCMonth() + 1) + "-"
         +  pad("00", d.getUTCDate()) + "T"
         +  pad("00", d.getUTCHours()) + ":"
         +  pad("00", d.getUTCMinutes()) + ":"
         +  pad("00", d.getUTCSeconds()) + "Z";
    },

    empty: empty,
  };
}();
