define([
  "lodash",
  "./query_def",
  "./time"
],
function (_, queryDef,time) {
  'use strict';

  function ElasticResponse(targets, response) {
    this.targets = targets;
    this.response = response;
  }

  ElasticResponse.prototype.processMetrics = function(esAgg, target, seriesList, props) {
    var metric, y, i, newSeries, bucket, value;

    for (y = 0; y < target.metrics.length; y++) {
      metric = target.metrics[y];
      if (metric.hide) {
        continue;
      }

      var timeShift = 0;
      if(target.timeShiftComparison && target.timeShiftComparison !== "") {
        timeShift +=  time.calcTimeShift(target.timeShiftComparison);
      }

      switch(metric.type) {
        case 'count': {
          newSeries = { datapoints: [], metric: 'count', props: props};
          for (i = 0; i < esAgg.buckets.length; i++) {
            bucket = esAgg.buckets[i];
            value = bucket.doc_count;
            newSeries.datapoints.push([value, bucket.key + timeShift]);
          }
          seriesList.push(newSeries);
          break;
        }
        case 'percentiles': {
          if (esAgg.buckets.length === 0) {
            break;
          }

          var firstBucket = esAgg.buckets[0];
          var percentiles = firstBucket[metric.id].values;

          for (var percentileName in percentiles) {
            newSeries = {datapoints: [], metric: 'p' + percentileName, props: props, field: metric.field};

            for (i = 0; i < esAgg.buckets.length; i++) {
              bucket = esAgg.buckets[i];
              var values = bucket[metric.id].values;
              newSeries.datapoints.push([values[percentileName], bucket.key + timeShift]);
            }
            seriesList.push(newSeries);
          }

          break;
        }
        case 'extended_stats': {
          for (var statName in metric.meta) {
            if (!metric.meta[statName]) {
              continue;
            }

            newSeries = {datapoints: [], metric: statName, props: props, field: metric.field};

            for (i = 0; i < esAgg.buckets.length; i++) {
              bucket = esAgg.buckets[i];
              var stats = bucket[metric.id];

              // add stats that are in nested obj to top level obj
              stats.std_deviation_bounds_upper = stats.std_deviation_bounds.upper;
              stats.std_deviation_bounds_lower = stats.std_deviation_bounds.lower;

              newSeries.datapoints.push([stats[statName], bucket.key] + timeShift);
            }

            seriesList.push(newSeries);
          }

          break;
        }
        default: {
          newSeries = { datapoints: [], metric: metric.type, field: metric.field, props: props};
          for (i = 0; i < esAgg.buckets.length; i++) {
            bucket = esAgg.buckets[i];

            value = bucket[metric.id];
            if (value !== undefined) {
              if (value.normalized_value) {
                newSeries.datapoints.push([value.normalized_value, bucket.key + timeShift]);
              } else {
                newSeries.datapoints.push([value.value, bucket.key + timeShift]);
              }
            }

          }
          seriesList.push(newSeries);
          break;
        }
      }
    }
  };

  ElasticResponse.prototype.processAggregationDocs = function(esAgg, aggDef, target, docs, props) {
    var metric, y, i, bucket, metricName, doc;

    for (i = 0; i < esAgg.buckets.length; i++) {
      bucket = esAgg.buckets[i];
      doc = _.defaults({}, props);
      doc[aggDef.field] = bucket.key;
      var refId = target.refId;

      for (y = 0; y < target.metrics.length; y++) {
        metric = target.metrics[y];

        switch(metric.type) {
          case "count": {
            metricName = metric.type;
            doc[metricName + " " + refId] = bucket.doc_count;
            break;
          }
          case 'extended_stats': {
            for (var statName in metric.meta) {
              if (!metric.meta[statName]) {
                continue;
              }

              var stats = bucket[metric.id];
              // add stats that are in nested obj to top level obj
              stats.std_deviation_bounds_upper = stats.std_deviation_bounds.upper;
              stats.std_deviation_bounds_lower = stats.std_deviation_bounds.lower;

              metricName = statName;
              doc[metricName + " " + metric.field + " " + refId] = stats[statName];
            }
            break;
          }
          case "calc_metric": {
            metricName = metric.type;
            doc[metricName + " " + metric.formula + " " + refId] = bucket[metric.id].value;
            break;
          }
          default:  {
            metricName = metric.type;
            doc[metricName + " " + metric.field + " " + refId] =bucket[metric.id].value;
            break;
          }
        }
      }

      docs.push(doc);
    }
  };

  // This is quite complex
  // neeed to recurise down the nested buckets to build series
  ElasticResponse.prototype.processBuckets = function(aggs, target, seriesList, docs, props, depth) {
    var bucket, aggDef, esAgg, aggId;
    var maxDepth = target.bucketAggs.length-1;

    for (aggId in aggs) {
      aggDef = _.find(target.bucketAggs, {id: aggId});
      esAgg = aggs[aggId];

      if (!aggDef) {
        continue;
      }

      if (depth === maxDepth) {
        if (aggDef.type === 'date_histogram')  {
          this.processMetrics(esAgg, target, seriesList, props);
        } else {
          this.processAggregationDocs(esAgg, aggDef, target, docs, props);
        }
      } else {
        for (var nameIndex in esAgg.buckets) {
          bucket = esAgg.buckets[nameIndex];
          props = _.clone(props);
          if (bucket.key !== void 0) {
            props[aggDef.field] = bucket.key;
          } else {
            props["filter"] = nameIndex;
          }
          if (bucket.key_as_string) {
            props[aggDef.field] = bucket.key_as_string;
          }
          this.processBuckets(bucket, target, seriesList, docs, props, depth+1);
        }
      }
    }
  };

  ElasticResponse.prototype._getMetricName = function(metric) {
    var metricDef = _.find(queryDef.metricAggTypes, {value: metric});
    if (!metricDef)  {
      metricDef = _.find(queryDef.extendedStats, {value: metric});
    }

    return metricDef ? metricDef.text : metric;
  };

  ElasticResponse.prototype._getSeriesName = function(series, target, metricTypeCount) {
    var metricName = this._getMetricName(series.metric);

    if (target.alias) {
      var regex = /\{\{([\s\S]+?)\}\}/g;

      return target.alias.replace(regex, function(match, g1, g2) {
        var group = g1 || g2;

        if (group.indexOf('term ') === 0) { return series.props[group.substring(5)]; }
        if (series.props[group] !== void 0) { return series.props[group]; }
        if (group === 'metric') { return metricName; }
        if (group === 'field') { return series.field; }

        return match;
      });
    }

    if (series.field && queryDef.isPipelineAgg(series.metric)) {
      var appliedAgg = _.find(target.metrics, { id: series.field });
      if (appliedAgg) {
        metricName += ' ' + queryDef.describeMetric(appliedAgg);
      } else {
        metricName = 'Unset';
      }
    } else if (series.field) {
      metricName += ' ' + series.field;
    }

    var propKeys = _.keys(series.props);
    if (propKeys.length === 0) {
      return metricName;
    }

    var name = '';
    for (var propName in series.props) {
      name += series.props[propName] + ' ';
    }

    if (metricTypeCount === 1) {
      return name.trim();
    }

    return name.trim() + ' ' + metricName;
  };

  ElasticResponse.prototype.nameSeries = function(seriesList, target) {
    var metricTypeCount = _.uniq(_.map(seriesList, 'metric')).length;
    var fieldNameCount = _.uniq(_.map(seriesList, 'field')).length;

    for (var i = 0; i < seriesList.length; i++) {
      var series = seriesList[i];
      series.target = this._getSeriesName(series, target, metricTypeCount, fieldNameCount);
    }
  };

  ElasticResponse.prototype.processHits = function(hits, seriesList, aliasDictionary) {
    var series = {target: 'docs', type: 'docs', datapoints: [], total: hits.total};
    var propName, hit, doc, i;

    for (i = 0; i < hits.hits.length; i++) {
      hit = hits.hits[i];
      doc = {
        _id: hit._id,
        _type: hit._type,
        _index: hit._index
      };

      if (hit._source) {
        for (propName in hit._source) {
          if (propName in aliasDictionary) {
            doc[aliasDictionary[propName]] = hit._source[propName];
          } else {
            doc[propName] = hit._source[propName];
          }
        }
      }

      for (propName in hit.fields) {
        doc[propName] = hit.fields[propName];
      }
      series.datapoints.push(doc);
    }

    seriesList.push(series);
  };

  ElasticResponse.prototype.trimDatapoints = function(aggregations, target) {
    var histogram = _.find(target.bucketAggs, { type: 'date_histogram'});

    var shouldDropFirstAndLast = histogram && histogram.settings && histogram.settings.trimEdges;
    if (shouldDropFirstAndLast) {
      var trim = histogram.settings.trimEdges;
      for(var prop in aggregations) {
        var points = aggregations[prop];
        if (points.datapoints.length > trim * 2) {
          points.datapoints = points.datapoints.slice(trim, points.datapoints.length - trim);
        }
      }
    }
  };

  ElasticResponse.prototype.getErrorFromElasticResponse = function(response, err) {
    var result = {};
    result.data = JSON.stringify(err, null, 4);
    if (err.root_cause && err.root_cause.length > 0 && err.root_cause[0].reason) {
      result.message = err.root_cause[0].reason;
    } else {
      result.message = err.reason || 'Unkown elatic error response';
    }

    if (response.$$config) {
      result.config = response.$$config;
    }

    return result;
  };

  ElasticResponse.prototype.getTimeSeries = function() {
    var seriesList = [];
    var options = {};
    var docs = [];
    options.docCountList = [];
    options.hasDocs = false;
    options.alias = {};

    for (var i = 0; i < this.response.responses.length; i++) {
      var response = this.response.responses[i];
      if (response.error) {
        throw this.getErrorFromElasticResponse(this.response, response.error);
      }

      if(!this.targets[i].metrics[0].aliasDic) {
        this.targets[i].metrics[0].aliasDic = {};
      }

      if (response.hits && response.hits.hits.length > 0) {
        this.processHits(response.hits, seriesList, this.targets[i].metrics[0].aliasDic);
      }

      if (response.aggregations) {
        var aggregations = response.aggregations;
        var target = this.targets[i];
        var tmpSeriesList = [];

        this.processBuckets(aggregations, target, tmpSeriesList, docs, {}, 0);
        this.trimDatapoints(tmpSeriesList, target);
        this.nameSeries(tmpSeriesList, target);

        options.docCountList[i] = docs.length;

        if(target.alias) {
          switch(target.metrics[0].type) {
            case "calc_metric": {
              options.alias[(target.metrics[0].type + " " + target.metrics[0].formula + " " + target.refId).toLowerCase()] = target.alias;
              break;
            }
            case "count": {
              options.alias[(target.metrics[0].type + " " + target.refId).toLowerCase()] = target.alias;
              break;
            }
            default: {
              options.alias[(target.metrics[0].type + " " + target.metrics[0].field + " " + target.refId).toLowerCase()] = target.alias;
              break;
            }
          }
        }

        for (var y = 0; y < tmpSeriesList.length; y++) {
          seriesList.push(tmpSeriesList[y]);
        }

        if (seriesList.length === 0 && docs.length > 0) {
          options.hasDocs = true;
          seriesList.push({target: 'docs', type: 'docs', datapoints: docs});
        }
      }
    }

    if(options.hasDocs)  {
      options.columns = {};
      options.hasDocs = false;
      for(var j = 0;j< seriesList[0].datapoints.length;j++) {
        Object.keys(seriesList[0].datapoints[j]).forEach(function(key) {
          if (Object.prototype.hasOwnProperty.call(options.columns, key)) {
            options.columns[key] += 1;
          } else {
            options.columns[key] = 1;
          }
        });
      }
      options.groupKey = Object.keys(options.columns).reduce(function(a, b) {
        return options.columns[a] > options.columns[b] ? a : b;
      });
      var documents = {};
      options.initTarget = this.targets;
      options.initResponse = this.response;
      options.multipleGroupedDimension= '';
      options.queryPointer = 0;
      options.queryResponseCount = 0;
      for(var l = 0;l< seriesList[0].datapoints.length;l++) {
        options.multipleGroupedDimensionArray = [];
        options.queryResponseCount = options.docCountList[options.queryPointer];
        if(l === options.queryResponseCount) {
          options.queryPointer++;
        }
        if(options.multipleGroupedDimensionArray.length===0) {
          options.initTarget[options.queryPointer].bucketAggs.forEach(function(arrElement) {
            options.multipleGroupedDimensionArray.push(seriesList[0].datapoints[l][arrElement.field]);
          });
        }
        options.multipleGroupedDimension = options.multipleGroupedDimensionArray.join('-');
        Object.keys(seriesList[0].datapoints[l]).forEach(function(key) {
          var k = key.toLowerCase();
          if (Object.prototype.hasOwnProperty.call(options.alias, k)) {
            k = options.alias[k];
          }
          if (Object.prototype.hasOwnProperty.call(documents, options.multipleGroupedDimension)) {
            documents[options.multipleGroupedDimension][k] = seriesList[0].datapoints[l][key];
          } else {
            var tempObj = {};
            tempObj[k] = seriesList[0].datapoints[l][key];
            documents[options.multipleGroupedDimension] = tempObj;
          }
        });
      }
      var datapointsArr = [];
      Object.keys(documents).forEach(function(key) {
        datapointsArr.push(documents[key]);
      });
      seriesList[0].datapoints = datapointsArr;
    }

    return { data: seriesList };
  };

  return ElasticResponse;
});
