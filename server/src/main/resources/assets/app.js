var app = angular.module("exprofile", ["ui", "ngRoute", "ngResource"]);

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

app.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/profile/:id', {
      templateUrl: 'profile.html',
      controller: 'ExhibitController'
    });
}]);

app.factory('Exhibit', ['$resource', function($resource) {
  var exhibit = $resource('api/exhibit', {}, {});
  exhibit.find = function(id, callback) {
    this.get({id: id}, callback);
    this.id = id;
  };
  exhibit.getId = function() {
    return this.id;
  };
  return exhibit;
}]);

app.factory('Compute', ['$http', '$log', function($http, $log) {
  var compute = {};
  compute.single = function(id, code, callback) {
    $http.post('api/compute', {id: id, code: code}).
      success(function(data, status, headers, config) { callback(data); }).
      error(function(data, status, headers, config) {
        $log.log("ERROR!");
        $log.log(data);
      });
  };
  return compute;
}]);

app.controller("CodeController", ["$scope", "$log", "Exhibit", "Compute", function($scope, $log, Exhibit, Compute) {
  $scope.code = "/** SQL goes here **/";
  $scope.hasResults = false;
  $scope.results = {data: [], columns: []};
  $scope.single = function() {
    Compute.single(Exhibit.getId(), this.code, function(data) {
      $scope.results = data;
      $scope.hasResults = true;
      $scope.$emit('newResults');
    });
  };
  $scope.cached = function() {
    $log.log("Cached " + Exhibit.getId() + ": " + this.code);
  };
  $scope.all = function() {
    $log.log("All " + Exhibit.getId() + ": " + this.code);
  };
}]);

app.controller("ExhibitController", ["$scope", "$log", "$routeParams", "Exhibit", function($scope, $log, $routeParams, Exhibit) {
  $scope.$emit('newExhibit', {id: $routeParams.id});
  $scope.active = {};
  Exhibit.find($routeParams.id, function(data) {
    $scope.columns = data.columns;
    $scope.frames = data.frames;
    // TODO: should configure a preferred ordering here.
    $scope.frameNames = Object.keys($scope.frames);
    for (var i = 0; i < $scope.frameNames.length; i++) {
      $scope.active[$scope.frameNames[i]] = false;
    }
    $scope.active[$scope.frameNames[0]] = true;

    $scope.attrs = data.attrs;
    var rawKeys = Object.keys($scope.attrs);
    $scope.attrKeys = [];
    for (var i = 0; i < rawKeys.length; i++) {
      var key = rawKeys[i];
      if (key === "fname" || key === "lname") {
        // Ignore
      } else {
        $scope.attrKeys.push(key);
      }
    }
  });

  $scope.setActive = function(frame) {
    for (var key in this.active) {
      this.active[key] = false;
    }
    this.active[frame] = true;
  };
}]);

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

app.directive("profileTables", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-tables.html',
  }; 
});

app.directive("datatable", ["$log", function($log) {
  return {
    restrict: 'A',
    link: function($scope, $elem, attrs) {
      var data = $scope.frames[attrs.name];
      var cols = $scope.columns[attrs.name];
      var ret = cols.map(function(col) { return $('<td>').append(col); })
      var thead = $('<thead>').append($('<tr>').append(ret));
      $elem.append(thead);
      var options = {'data': data};
      options.scrollX = true;
      options.filter = false;
      $elem.DataTable(options); 
    }
  };
}]);

app.directive("resulttable", ["$log", function($log) {
  return {
    restrict: 'A',
    link: function($scope, $elem, attrs) {
      $scope.$on('newResults', function(args) {
        if ($.fn.dataTable.isDataTable($elem)) {
          $elem.dataTable().fnDestroy();
          $elem.empty();
        }
        var data = $scope.results.data;
        var cols = $scope.results.columns;
        var ret = cols.map(function(col) { return $('<td>').append(col); })
        var thead = $('<thead>').append($('<tr>').append(ret));
        $elem.append(thead);
        var options = {'data': data};
        options.scrollX = true;
        options.scrollY = "200px";
        options.scrollCollapse = true;
        options.paging = false;
        options.filter = false;
        $elem.DataTable(options);
      });
    }
  };
}]);
