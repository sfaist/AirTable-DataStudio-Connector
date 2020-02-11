var primary_name = 'unique_id';

var regExp = new RegExp("^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])$");

var fields;
var data;

function getConfig() {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  config.newTextInput()
    .setId('key')
    .setName('Airtable API Key')
    .setPlaceholder('Your API Key');
  config.newTextInput()
    .setId('id')
    .setName('Airtable Base ID')
    .setPlaceholder('Your Base ID');
  config.newTextInput()
    .setId('resource')
    .setName('Table Name')
    .setPlaceholder('The name of the table');  
  return config.build();
}


function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  if(!data){
    data = fetchData(request);
  }
  fields.newDimension()
    .setId(primary_name)
    .setName(primary_name)
    .setType(types.TEXT);
  var max = data.length>50?50:data.length;
  for(var i =0;i<max;i++){
  Object.keys(data[i].fields).forEach(function(key) {
    var val = data[i].fields[key];
    var id = key.replace(/[^a-zA-Z0-9]+/g,"");
    var tp = typeof val;
    if(fields.getFieldById(id)!=null){
      return;
    }
    if(typeof val == 'number'){
      fields.newMetric()
      .setId(id)
      .setName(key)
      .setType(types.NUMBER)
      .setIsReaggregatable(true)
      .setAggregation(aggregations.SUM);
    }
    else if(isDate(val)){
      fields.newDimension()
      .setId(id)
      .setName(key)
      .setType(types.YEAR_MONTH_DAY)
      .setGroup('DATETIME');
    }
    else{
      fields.newDimension()
      .setId(id)
      .setName(key)
      .setType(types.TEXT);
    }
  });
  }
  return fields;
}
function getSchema(request) {
  if(isAdminUser()) log({message: 'start getschema', initialData: JSON.stringify({ 'message': request })});
  if(!fields){
    fields = getFields(request).build();
  }
  if(isAdminUser()) log({message: 'end getschema', initialData: JSON.stringify({ 'schema': fields })});
  return { 'schema': fields };
}
function getData(request) {
  if(isAdminUser()) log({message: 'start getdata', initialData: JSON.stringify(request)});
  var dataSchema = [];
  var schema = getSchema(request).schema;
  //build dataschema
  request.fields.forEach(function(reqField) {
    for(var i = 0; i < schema.length; i++){
      var c = schema[i];
      if(c.name == reqField.name){
        dataSchema.push(c);
        break;
      }
    }
  });
  //fetch data
  if(!data){
    data = fetchData(request);
  }  
  //filter results
  var result = [];
  for(var i =0;i<data.length;i++){
    var current = {values:[]};
    for(var j = 0; j < dataSchema.length; j++){
      var c = dataSchema[j];
      if(c.label==primary_name){
        current.values.push(data[i].id);
      }
      else if(c.semantics.semanticType=='YEAR_MONTH_DAY'){
        current.values.push(getDate(data[i].fields[c.label]));
      }
      else if(Array.isArray(data[i].fields[c.label])){
        current.values.push(data[i].fields[c.label].join(','));
      }
      else{
        current.values.push(data[i].fields[c.label]);
      }
    }
    result.push(current);
  }
  
  if(isAdminUser()) log({message: 'end getdata', initialData: JSON.stringify({schema: dataSchema, rows: result})});
  return {schema: dataSchema, rows: result};
}
function fetchData(request){
  var offset = '';
  var data = [];
  do {
    var url = [      
      'https://api.airtable.com/v0/',
      request.configParams.id,
      '/',
      request.configParams.resource,
      '?api_key=',
      request.configParams.key
      ,offset];
    var response = JSON.parse(UrlFetchApp.fetch(url.join('')));
    offset=response.offset? '&offset=' + response.offset : '';
    data = data.concat(response.records);
  } while (offset!='');
  return data;
}
function getDataTest(){
  var result = getData({configParams: {resource : 'x', key: 'x', id:'x'}, fields: [{name: primary_name},{name: 'BarsCKG'},{name:'CollectionDate'}]});
  console.log({message: 'end datatest', initialData: JSON.stringify(data)});
}
function getSchemaTest(){
  var result = getSchema({"configParams":{"resource":"x","id":"x","key":"x"}});
  console.log({message: 'end schematest', initialData: JSON.stringify(data)});
}

function getAuthType() {
  var response = {
    type: 'NONE',
  };
  return response;
}

function isAdminUser() {
  return false;
}
function log(info){
  if(isAdminUser()){
    console.log(info);
  }
}
function isDate(val){
  return regExp.test(val);
}
function getDate(val){
  if(regExp.test(val)){
    var test = regExp.exec(val);
    return test[1]+''+test[2]+''+test[3];
  }
  return "20000101";
}