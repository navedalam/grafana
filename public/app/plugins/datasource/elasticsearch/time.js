define([
  'jquery'
],

function() {
  'use strict';
  var time = {};

  time.calcTimeShift = function(shift) {
    var unit = shift.slice(-1);
    var unitShift = 0;
    switch(unit) {
      case 'm': {
        unitShift = 60*1000;
        break;
      }
      case 'h': {
        unitShift = 60*60*1000;
        break;
      }
      case 'd': {
        unitShift = 24*60*60*1000;
        break;
      }
      case 'w': {
        unitShift = 7*24*60*60*1000;
        break;
      }
      case 'M': {
        unitShift = 30*24*60*60*1000;
        break;
      }
      case 'Y': {
        unitShift = 365*24*60*60*1000;
        break;
      }
    }
    return parseInt(shift.slice(0,-1)*unitShift);
  };

  time.timeZoneShift = function() {
    return 5.5*60*60*1000;
  };

  time.getMonthStartTime = function(epochTime) {
    var d = new Date(0);
    d.setUTCSeconds(epochTime);
    var firstDay = new Date(d.getUTCFullYear(), d.getUTCMonth(), 1);
    return new Date(firstDay.getTime() - firstDay.getTimezoneOffset() * 60000).getTime();
  };

  return time;
});
