var app = angular.module("exprofile", ["ui"]);

app.value('ui.config', {
  codemirror: {
    mode: 'text/x-sql',
    lineNumbers: true,
    extraKeys: {"Ctrl-Space": "autocomplete"},
    hint: CodeMirror.hint.sql,
    hintOptions: {
      tables: {
        "txns": ["label"],
        "calls": ["label"],
        "visits": ["label"],
      }
    },
  }
});

app.controller("ExhibitController", function() {
  this.attrs = exhibitData.attrs;
  this.frames = exhibitData.frames;
  this.code = "/** SQL goes here **/";
  this.showEditor = false;
});

app.directive("navbar", function() {
  return {
    restrict: 'E',
    templateUrl: 'navbar.html'
  };
});

app.directive("profileAttrs", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-attrs.html',
  };
});

app.directive("profileMetrics", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-metrics.html',
  };
});

app.directive("profileFeed", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-feed.html',
    controller: ['$log', function($log){
      this.sortFields = {
        txns: 'tstamp',
        calls: 'tstamp',
        visits: 'tstamp'
      };
      this.getFeed = function(frames) {
        var feed = [];
        for (var key in frames) {
          if (frames.hasOwnProperty(key)) {
            var sk = this.sortFields[key];
            for (var i = 0; i < frames[key].length; i++) {
              var obs = frames[key][i];
              obs['frame'] = key;
              obs['sortOn'] = obs[sk];
              feed.push(obs);
            }
          }
        }
        feed.sort(feedCmp);
        return feed;
      };
    }],
    controllerAs: 'feedCtrl'
 }; 
});

var feedCmp = function(a, b) {
  if (a['sortOn'] < b['sortOn'])
    return 1;
  if (a['sortOn'] > b['sortOn'])
    return -1;
  return 0; 
};

var exhibitData = {
  attrs: {
    name: "Josh Wills",
    age: 35,
    city: "San Francisco",
    state: "CA",
    zip: 94117,
  },
  frames: {
    txns: [
      {tstamp: 1, label: "This is a transaction." },
      {tstamp: 10, label: "This is a later transaction." },
    ],
    calls: [
      {tstamp: 4, label: "This is a phone call." },
    ],
    visits: [
      {tstamp: 5, label: "This is a web visit." },
      {tstamp: 20, label: "This is a later web visit." },
    ],
  },
};
