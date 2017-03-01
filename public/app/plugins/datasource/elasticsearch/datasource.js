define([
  'angular',
  'lodash',
  'moment',
  'app/core/utils/kbn',
  './query_builder',
  './index_pattern',
  './elastic_response',
  './query_ctrl',
  './time',
],
function (angular, _, moment, kbn, ElasticQueryBuilder, IndexPattern, ElasticResponse, a, time) {
  'use strict';

  /** @ngInject */
  function ElasticDatasource(instanceSettings, $q, backendSrv, templateSrv, timeSrv) {
    this.basicAuth = instanceSettings.basicAuth;
    this.withCredentials = instanceSettings.withCredentials;
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.index = instanceSettings.index;
    this.timeField = instanceSettings.jsonData.timeField;
    this.esVersion = instanceSettings.jsonData.esVersion;
    this.indexPattern = new IndexPattern(instanceSettings.index, instanceSettings.jsonData.interval);
    this.interval = instanceSettings.jsonData.timeInterval;
    this.queryBuilder = new ElasticQueryBuilder({
      timeField: this.timeField,
      esVersion: this.esVersion,
    });

    this._request = function(method, url, data) {
      var options = {
        url: this.url + "/" + url,
        method: method,
        data: data
      };

      if (this.basicAuth || this.withCredentials) {
        options.withCredentials = true;
      }
      if (this.basicAuth) {
        options.headers = {
          "Authorization": this.basicAuth
        };
      }

      return backendSrv.datasourceRequest(options);
    };

    this._get = function(url) {
      var range = timeSrv.timeRange();
      var index_list = this.indexPattern.getIndexList(range.from.valueOf(), range.to.valueOf());
      if (_.isArray(index_list) && index_list.length) {
        return this._request('GET', index_list[0] + url).then(function(results) {
          results.data.$$config = results.config;
          return results.data;
        });
      } else {
        return this._request('GET', this.indexPattern.getIndexForToday() + url).then(function(results) {
          results.data.$$config = results.config;
          return results.data;
        });
      }
    };

    this._post = function(url, data) {
      return this._request('POST', url, data).then(function(results) {
        results.data.$$config = results.config;
        return results.data;
      });
    };

    this.annotationQuery = function(options) {
      var annotation = options.annotation;
      var timeField = annotation.timeField || '@timestamp';
      var queryString = annotation.query || '*';
      var tagsField = annotation.tagsField || 'tags';
      var titleField = annotation.titleField || 'desc';
      var textField = annotation.textField || null;

      var range = {};
      range[timeField]= {
        from: options.range.from.valueOf(),
        to: options.range.to.valueOf(),
        format: "epoch_millis",
      };

      var queryInterpolated = templateSrv.replace(queryString, {}, 'lucene');
      var query = {
        "bool": {
          "filter": [
            { "range": range },
            {
              "query_string": {
                "query": queryInterpolated
              }
            }
          ]
        }
      };

      var data = {
        "query" : query,
        "size": 10000
      };

      // fields field not supported on ES 5.x
      if (this.esVersion < 5) {
        data["fields"] = [timeField, "_source"];
      }

      var header = {search_type: "query_then_fetch", "ignore_unavailable": true};

      // old elastic annotations had index specified on them
      if (annotation.index) {
        header.index = annotation.index;
      } else {
        header.index = this.indexPattern.getIndexList(options.range.from, options.range.to);
      }

      var payload = angular.toJson(header) + '\n' + angular.toJson(data) + '\n';

      return this._post('_msearch', payload).then(function(res) {
        var list = [];
        var hits = res.responses[0].hits.hits;

        var getFieldFromSource = function(source, fieldName) {
          if (!fieldName) { return; }

          var fieldNames = fieldName.split('.');
          var fieldValue = source;

          for (var i = 0; i < fieldNames.length; i++) {
            fieldValue = fieldValue[fieldNames[i]];
            if (!fieldValue) {
              console.log('could not find field in annotation: ', fieldName);
              return '';
            }
          }

          if (_.isArray(fieldValue)) {
            fieldValue = fieldValue.join(', ');
          }
          return fieldValue;
        };

        for (var i = 0; i < hits.length; i++) {
          var source = hits[i]._source;
          var time = source[timeField];
          if (typeof hits[i].fields !== 'undefined') {
            var fields = hits[i].fields;
            if (_.isString(fields[timeField]) || _.isNumber(fields[timeField])) {
              time = fields[timeField];
            }
          }

          var event = {
            annotation: annotation,
            time: moment.utc(time).valueOf(),
            title: getFieldFromSource(source, titleField),
            tags: getFieldFromSource(source, tagsField),
            text: getFieldFromSource(source, textField)
          };

          list.push(event);
        }
        return list;
      });
    };

    this.testDatasource = function() {
      timeSrv.setTime({ from: 'now-1m', to: 'now' });
      return this._get('/_stats').then(function() {
        return { status: "success", message: "Data source is working", title: "Success" };
      }, function(err) {
        if (err.data && err.data.error) {
          return { status: "error", message: angular.toJson(err.data.error), title: "Error" };
        } else {
          return { status: "error", message: err.status, title: "Error" };
        }
      });
    };

    this.getQueryHeader = function(searchType, timeFrom, timeTo) {
      var header = {search_type: searchType, "ignore_unavailable": true};
      header.index = this.indexPattern.getIndexList(timeFrom, timeTo);
      return angular.toJson(header);
    };

    this.query = function(options) {
      var payload = "";
      var target;
      var sentTargets = [];
      options.calcMetric = {};
      options.calcMetric.status = false;
      options.calcMetric.formulas = [];
      options.calcMetric.queries = [];
      options.calcMetric.typeDate = [];
      options.timeShift = {};
      options.mtd = {};
      options.mtd.queryList = [];
      options.mtd.targetList = [];

      for (var target_index = 0; target_index < options.targets.length; target_index++) {
        target = options.targets[target_index];
        if(target.metrics) {
          if(target.metrics[0].type === 'calc_metric') {
            options.calcMetric.status = true;
          }
        }
      }

      // add global adhoc filters to timeFilter
      var adhocFilters = templateSrv.getAdhocFilters(this.name);

      for (var i = 0; i < options.targets.length; i++) {
        target = options.targets[i];
        if (target.hide) {continue;}

        var queryString = templateSrv.replace(target.query || '*', options.scopedVars, 'lucene');
        queryString = queryString.replace(" and ", " AND ").replace(" or "," OR ").replace(" not "," NOT ");
        queryString = queryString.replace(new RegExp("[AND |OR |OR NOT |AND NOT ]*[A-Za-z_0-9]*:RemoveWildcard","gm"),"");
        queryString = queryString.trim();
        if(queryString.startsWith('AND') || queryString.startsWith("OR")){
          queryString = queryString.substr(queryString.indexOf(" ") + 1);
        }
        if(queryString === ""){
          queryString = "*";
        }
        var queryObj = this.queryBuilder.build(target, adhocFilters, queryString);
        var esQuery = angular.toJson(queryObj);

        var searchType = (queryObj.size === 0 && this.esVersion < 5) ? 'count' : 'query_then_fetch';
        var header = this.getQueryHeader(searchType, options.range.from, options.range.to);
        //payload +=  header + '\n';

        //payload += esQuery + '\n';
        var tempPayload="";
        if(target.metrics) {
          if(target.metrics[0].type === 'calc_metric') {
            for(var mtdTarget = 0; mtdTarget < options.mtd.targetList.length; mtdTarget++) {
              options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$__interval/g,"10000d");
              options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$timeFrom/g,time.getMonthStartTime(options.range.to.valueOf()/1000));
              options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$timeTo/g, options.range.to.valueOf() + 5.5*3600000);
              sentTargets.push(options.mtd.targetList[mtdTarget]);
              payload += options.mtd.queryList[mtdTarget];
            }
            options.mtd.offset = options.mtd.queryList.length;
            options.mtd.queryList = [];
            options.mtd.targetList = [];

            tempPayload += "";
            if(!target.metrics[0].formula || target.metrics[0].formula === "") {
              target.metrics[0].formula = "query1 * 1";
            }
            options.calcMetric.queries.push(i + options.mtd.offset);
            options.calcMetric.formulas.push(target.metrics[0].formula);
            if(target.bucketAggs[0].type === "date_histogram") {
              options.calcMetric.typeDate.push(true);
            }
            else {
              options.calcMetric.typeDate.push(false);
            }
          } else if(options.targets[0].editQueryMode) {
            tempPayload += options.targets[0].rawQuery.replace(/(\r\n|\n|\r)/gm,"") + '\n';
            tempPayload +=  header + '\n';
          } else {
            tempPayload +=  header + '\n';
            tempPayload += esQuery + '\n';
          }

        }
        if(target.mtd === true) {
          var tempTarget = angular.copy(target);
          tempTarget.alias = tempTarget.alias + " MTD";
          tempTarget.isMTD = true;
          tempTarget.isMTDOf = i;
          options.mtd.queryList.push(tempPayload);
          options.mtd.targetList.push(tempTarget);
        }
        if(target.timeShiftComparison && target.timeShiftComparison !== "") {
          tempPayload = tempPayload.replace(/\$__interval/g, options.interval);
          tempPayload = tempPayload.replace(/\$timeFrom/g, options.range.from.valueOf() - time.calcTimeShift(target.timeShiftComparison) + 5.5*3600000);
          tempPayload = tempPayload.replace(/\$timeTo/g, options.range.to.valueOf() - time.calcTimeShift(target.timeShiftComparison)  + 5.5*3600000);
          options.timeShift[i] = time.calcTimeShift(target.timeShiftComparison);
        } else {
          tempPayload = tempPayload.replace(/\$__interval/g, options.interval);
          tempPayload = tempPayload.replace(/\$timeFrom/g, options.range.from.valueOf() + 5.5*3600000);
          tempPayload = tempPayload.replace(/\$timeTo/g, options.range.to.valueOf() + 5.5*3600000);
        }
        payload += tempPayload;
        sentTargets.push(target);
      }

      for(var mtdTarget = 0; mtdTarget < options.mtd.targetList.length; mtdTarget++) {
        options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$__interval/g,"10000d");
        options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$timeFrom/g,time.getMonthStartTime(options.range.to.valueOf()/1000));
        options.mtd.queryList[mtdTarget] = options.mtd.queryList[mtdTarget].replace(/\$timeTo/g, options.range.to.valueOf() + 5.5*3600000);
        sentTargets.push(options.mtd.targetList[mtdTarget]);
        payload += options.mtd.queryList[mtdTarget];
      }

      if (sentTargets.length === 0) {
        return $q.when([]);
      }

      payload = payload.replace(/\$timeFrom/g, options.range.from.valueOf());
      payload = payload.replace(/\$timeTo/g, options.range.to.valueOf());
      payload = templateSrv.replace(payload, options.scopedVars);

      return this._post('_msearch', payload).then(function(res) {
        for (i=0;i<res.responses.length;i++) {
          if(options.timeShift.hasOwnProperty(i) && options.targets[i].bucketAggs[0].type === "date_histogram") {
            var tmp = res.responses[i].aggregations[2].buckets;
            Object.keys(tmp).forEach(function(key) {
              tmp[key]['key'] = tmp[key]['key']+ options.timeShift[i];
              tmp[key]['key_as_string'] = tmp[key]['key'].toString();
            });
          }
          if(sentTargets[i].isMTD) {
            var tempResponse = res.responses[sentTargets[i].isMTDOf].aggregations[2].buckets;
            res.responses[i].aggregations[2].buckets[0].key = tempResponse[tempResponse.length-1].key;
            res.responses[i].aggregations[2].buckets[0].key_as_string = tempResponse[tempResponse.length-1].key_as_string;

          }
        }
        if(options.calcMetric.status) {
          options.calcMetric.status = false;
          var resArr = [];
          for (i=0;i<res.responses.length;i++) {
            if(options.calcMetric.queries.indexOf(i)>=0) {
              continue;
            }
            var responses = res.responses[i].aggregations[Object.keys(res.responses[i].aggregations)[0]].buckets;
            var customMetric = {};
            Object.keys(responses).forEach(function(response) {
              var metricValue = 0;
              var keys = Object.keys(responses[response]);
              if(keys.length === 3) {
                metricValue = responses[response].doc_count;
              } else {
                for(var elm = keys.length-1; elm--;) {
                  if (keys[elm] === "doc_count" || keys[elm] === 'key' || keys[elm] === 'key_as_string') {
                    keys.splice(elm, 1);
                  }
                }
                metricValue = responses[response][keys[0]].value;
              }
              if(customMetric[responses[response].key]) {
                customMetric[responses[response].key] += metricValue;
              } else {
                customMetric[responses[response].key] = metricValue;
              }
            });
            resArr.push(customMetric);
          }
          var resMap = {};
          var keySet = new Set();
          for (i=0;i<resArr.length;i++) {
            Object.keys(resArr[i]).forEach(function(key) {
              keySet.add(key);
            });
          }
          for (i=0;i<resArr.length;i++) {
            for(var key of keySet) {
              if(!resArr[i].hasOwnProperty(key)) {
                resArr[i][key]=0;
              }
              if(resMap[key]) {
                resMap[key].push(resArr[i][key]);
              } else {
                var arr = [];
                arr[0]=resArr[i][key];
                resMap[key] = arr;
              }
            }
          }
          for(var k=0;k<options.calcMetric.formulas.length;k++) {
            var formula = options.calcMetric.formulas[k];
            for(i=res.responses.length;i>0;i--) {
              var re = new RegExp("query"+(i), 'g');
              formula = formula.replace(re,"resMap[n]["+(i-1)+"]");
            }
            var finalMap = {};
            for (var n in resMap) {
              if (resMap.hasOwnProperty(n) && resMap[n].length === resArr.length) {
                finalMap[n] = eval(formula);
              }
            }
            var sortedKeys = Object.keys(finalMap).sort();
            var tempBucket = [];
            for (i = 0; i< sortedKeys.length;i++) {
              var tempObj = {};
              if(options.calcMetric.typeDate[k]) {
                tempObj = {"key": parseInt(sortedKeys[i]), "key_as_string": sortedKeys[i].toString(), "doc_count": 0, "1": {"value": finalMap[sortedKeys[i]]}};
              } else {
                tempObj = {"key": sortedKeys[i], "key_as_string": sortedKeys[i].toString(), "doc_count": 0, "1": {"value": finalMap[sortedKeys[i]]}};
              }
              tempBucket.push(tempObj);
            }
            var tempRes = (JSON.parse(JSON.stringify(res.responses[0])));
            tempRes.aggregations[2].buckets = tempBucket;
            tempRes.aggregations.isCustom = true;
            res.responses.push(tempRes);
          }
        }
        return new ElasticResponse(sentTargets, res).getTimeSeries();
      });
    };

    this.getFields = function(query) {
      return this._get('/_mapping').then(function(result) {

        var typeMap = {
          'float': 'number',
          'double': 'number',
          'integer': 'number',
          'long': 'number',
          'date': 'date',
          'string': 'string',
          'text': 'string',
          'scaled_float': 'number',
          'nested': 'nested'
        };

        function shouldAddField(obj, key, query) {
          if (key[0] === '_') {
            return false;
          }

          if (!query.type) {
            return true;
          }

          // equal query type filter, or via typemap translation
          return query.type === obj.type || query.type === typeMap[obj.type];
        }

        // Store subfield names: [system, process, cpu, total] -> system.process.cpu.total
        var fieldNameParts = [];
        var fields = {};

        function getFieldsRecursively(obj) {
          for (var key in obj) {
            var subObj = obj[key];

            // Check mapping field for nested fields
            if (subObj.hasOwnProperty('properties')) {
              fieldNameParts.push(key);
              getFieldsRecursively(subObj.properties);
            } else {
              var fieldName = fieldNameParts.concat(key).join('.');

              // Hide meta-fields and check field type
              if (shouldAddField(subObj, key, query)) {
                fields[fieldName] = {
                  text: fieldName,
                  type: subObj.type
                };
              }
            }
          }
          fieldNameParts.pop();
        }

        for (var indexName in result) {
          var index = result[indexName];
          if (index && index.mappings) {
            var mappings = index.mappings;
            for (var typeName in mappings) {
              var properties = mappings[typeName].properties;
              getFieldsRecursively(properties);
            }
          }
        }

        // transform to array
        return _.map(fields, function(value) {
          return value;
        });
      });
    };

    this.getTerms = function(queryDef) {
      var range = timeSrv.timeRange();
      var searchType = this.esVersion >= 5 ? 'query_then_fetch' : 'count' ;
      var header = this.getQueryHeader(searchType, range.from, range.to);
      var esQuery = angular.toJson(this.queryBuilder.getTermsQuery(queryDef));

      esQuery = esQuery.replace(/\$timeFrom/g, range.from.valueOf());
      esQuery = esQuery.replace(/\$timeTo/g, range.to.valueOf());
      esQuery = header + '\n' + esQuery + '\n';

      return this._post('_msearch?search_type=' + searchType, esQuery).then(function(res) {
        if (!res.responses[0].aggregations) {
          return [];
        }

        var buckets = res.responses[0].aggregations["1"].buckets;
        return _.map(buckets, function(bucket) {
          return {text: bucket.key, value: bucket.key};
        });
      });
    };

    this.metricFindQuery = function(query) {
      query = angular.fromJson(query);
      query.query = templateSrv.replace(query.query || '*', {}, 'lucene');

      if (!query) {
        return $q.when([]);
      }

      if (query.find === 'fields') {
        return this.getFields(query);
      }
      if (query.find === 'terms') {
        return this.getTerms(query);
      }
    };

    this.getTagKeys = function() {
      return this.getFields({});
    };

    this.getTagValues = function(options) {
      return this.getTerms({field: options.key, query: '*'});
    };
  }

  return {
    ElasticDatasource: ElasticDatasource
  };
});
