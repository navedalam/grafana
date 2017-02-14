///<reference path="../../headers/common.d.ts" />

import _ from 'lodash';
import kbn from 'app/core/utils/kbn';
import {Variable, assignModelProperties, variableTypes} from './variable';
import {VariableSrv} from './variable_srv';

export class HTTPVariable implements Variable {
  query: string;
  options: any;
  current: any;
  http: any;
  refresh: any;
  defaults = {
    type: 'http',
    name: '',
    label: '',
    hide: 0,
    refresh: 0,
    options: [],
    current: {},
    query: '',
  };

  /** @ngInject **/
  constructor(private model, private timeSrv, private templateSrv, private variableSrv, private $http) {
    assignModelProperties(this, model, this.defaults);
    this.http = $http;
  }

  setValue(option) {
    return this.variableSrv.setOptionAsCurrent(this, option);
  }

  getSaveModel() {
    assignModelProperties(this.model, this, this.defaults);
    return this.model;
  }

  updateOptions() {
    // extract options in comma separated string
    return this.getHttpVariableOptions(this).then(this.variableSrv.validateVariableSelectionState(this));
  }

  getHttpVariableOptions(variable) {
    return this.http({
      method: 'GET',
      url: variable.query,
      }).then(function successCallback(response) {
        return variable.options = _.sortBy(response.data, 'text');
      }, function errorCallback() {
        return variable.options = [{text: 'Failed to load http variable values', value: ''}];
      });
  }

  dependsOn(variable) {
    return false;
  }

  setValueFromUrl(urlValue) {
    return this.variableSrv.setOptionFromUrl(this, urlValue);
  }

  getValueForUrl() {
    return this.current.value;
  }
}

variableTypes['http'] = {
  name: 'HTTP',
  ctor: HTTPVariable,
  description: 'Fetch variable values' ,
  supportsMulti: false,
};
