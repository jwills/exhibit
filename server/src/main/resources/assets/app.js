var app = angular.module("exprofile", ["ui", "ngRoute", "ngResource"]);

app.value('ui.config', {
  codemirror: {
    mode: 'text/x-sql',
    lineNumbers: true,
    extraKeys: {"Ctrl-Space": "autocomplete"},
    hint: CodeMirror.hint.sql,
  }
});

app.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/ex/:entity/:id', {
      templateUrl: 'profile.html',
      controller: 'ProfileController',
      resolve: {
        profileData: function($route, Exhibit, $q) {
          var deferred = $q.defer();
          var params = $route.current.params;
          Exhibit.find(params.entity, params.id, function(data) {
            deferred.resolve(data);
          });
          return deferred.promise;
        }
      }
    });
}]);

app.factory('Exhibit', ['$resource', '$log', function($resource, $log) {
  var exhibit = $resource('api/exhibit/:entity/:id', {}, {});
  exhibit.find = function(entity, id, success) {
    $log.log("Looking up data for " + entity + " with id = " + id);
    this.id = {entity: entity, id: id};
    return this.get(this.id, success);
  };
  exhibit.getId = function() {
    return this.id;
  };
  return exhibit;
}]);

app.factory('Code', ['$resource', '$log', function($resource, $log) {
  return $resource('api/calculation', {}, {});
}]);

app.factory('Compute', ['$http', '$log', function($http, $log) {
  var compute = {};
  compute.single = function(id, code, callback) {
    $http.post('api/compute', {id: id, code: code}).
      success(function(data, status, headers, config) { callback(data); }).
      error(function(data, status, headers, config) {
        $log.log("ERROR on Compute.single: " + data);
      });
  };
  compute.cluster = function(id, code, callback) {
    $http.post('api/compute', {id: id, code: code}).
      success(function(data, status, headers, config) { callback(data); }).
      error(function(data, status, headers, config) {
        $log.log("ERROR on Compute.cluster: " + data);
      });
  };
  return compute;
}]);

app.factory('UIConfig', ['$http', '$log', function($http, $log) {
  var uiconfig = {
    player: {
      frameNames: ["passes", "rushes", "convs", "fgxp"],
      titleAttr: "pname",
      attrKeys: ["fname", "lname", "pos1", "pos2", "cteam", "height", "weight", "yob", "start"],
      frameEntities: {
        passes: { psr: "player", trg: "player" },
        rushes: { bc: "player" },
        convs: { psr: "player", trg: "player", bc: "player" },
        fgxp: { fkicker: "player" }
      }
    }
  };
  uiconfig.getEntities = function() {
    return ["player"];
  };
  return uiconfig;
}]);

app.controller("CodeController", ["$scope", "$log", "Exhibit", "Compute", "Code", function($scope, $log, Exhibit, Compute, Code) {
  $scope.code = "/** SQL goes here **/";
  $scope.hasResults = false;
  $scope.results = {data: [], columns: []};
  $scope.locally = function() {
    Compute.single(Exhibit.getId(), this.code, function(data) {
      $scope.results = data;
      $scope.hasResults = true;
      $scope.$emit('newResults');
    });
  };
  $scope.showCode = function(id) {
    $log.log("Lookup code for " + id);
    Code.get({id: id}, function(resp) {
      $scope.code = resp.code;
      $scope.locally();
    });
  };
  $scope.cluster = function() {
    Compute.cluster(Exhibit.getId(), this.code, function(data) {
      $log.log("Got response to cluster compute: " + data);
    });
  };
  $scope.save = function() {
    Code.save({code: this.code}, function() {});
  };
}]);

app.controller("ProfileController", function($scope, $log, Code, UIConfig, profileData) {
  $scope.active = {};
  $scope.uiconfig = UIConfig[profileData.id.entity];
  $scope.frameNames = $scope.uiconfig.frameNames;
  $scope.metrics = profileData.metrics;
  var exhibit = profileData.exhibit;
  $scope.columns = exhibit.columns;
  $scope.frames = exhibit.frames;
  for (var i = 0; i < $scope.frameNames.length; i++) {
    $scope.active[$scope.frameNames[i]] = false;
  }
  $scope.active[$scope.frameNames[0]] = true;

  var network = {};
  UIConfig.getEntities().map(function(e) { network[e] = {}; });
  var frameEntities = $scope.uiconfig.frameEntities;
  for (var i = 0; i < $scope.frameNames.length; i++) {
    var fn = $scope.frameNames[i];
    if (fn in frameEntities) {
      var columns = $scope.columns[fn];
      var indices = {};
      for (var j = 0; j < columns.length; j++) {
        if (columns[j] in frameEntities[fn]) {
          indices[j] = frameEntities[fn][columns[j]];
        }
      }
      var frame = $scope.frames[fn];
      for (var j = 0; j < frame.length; j++) {
        var row = frame[j];
        for (var k in indices) {
          var e = row[k];
          if (e !== null && e !== profileData.id.id) {
            var ec = network[indices[k]];
            if (e in ec) {
              ec[e]++;
            } else {
              ec[e] = 1;
            }
          }
        }
      }
    } 
  }

  $scope.network = {};
  for (var e in network) {
    var t = [];
    for (var key in network[e]) {
      t.push([key, network[e][key]]);
    }
    t.sort(function(a, b) {
      a = a[1];
      b = b[1];
      return a < b ? 1 : (a > b ? -1 : 0);
    });
    $scope.network[e] = t;
  }

  $scope.attrs = exhibit.attrs;
  $scope.profileTitle = $scope.attrs[$scope.uiconfig.titleAttr];
  $scope.attrKeys = $scope.uiconfig.attrKeys;

  $scope.setActive = function(frame) {
    for (var key in this.active) {
      this.active[key] = false;
    }
    this.active[frame] = true;
  };
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

app.directive("profileNetwork", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-network.html',
  };
});

app.directive("profileTables", function() {
  return {
    restrict: 'E',
    templateUrl: 'profile-tables.html',
  }; 
});

app.directive("editor", function() {
  return {
    restrict: 'E',
    templateUrl: 'editor.html',
  };
});

app.directive("datatable", ["$log", function($log) {
  return {
    restrict: 'A',
    link: function($scope, $elem, attrs) {
      var data = $scope.frames[attrs.name];
      var cols = $scope.columns[attrs.name];
      var colEntities = $scope.uiconfig.frameEntities[attrs.name];
      var columns = cols.map(function(col) {
        var ret = {title: col};
        if (col in colEntities) {
          ret.render = function(data, type, row, meta) {
            if ("display" === type) {
              return "<a href='#/ex/" + colEntities[col] + "/" + data + "'>" + data + "</a>";
            }
            return data;
          };
        }
        return ret;
      });
      var options = {'data': data, 'columns': columns};
      options.scrollX = true;
      options.scrollY = "400px";
      options.paging = false;
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
        var columns = cols.map(function(col) { return {'title': col}; })
        var options = {'data': data, 'columns': columns};
        options.scrollY = "200px";
        options.scrollCollapse = true;
        options.paging = false;
        options.filter = false;
        var table = $elem.DataTable(options);
        table.columns.adjust().draw();
      });
    }
  };
}]);
