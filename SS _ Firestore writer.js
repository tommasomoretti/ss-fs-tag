const Firestore = require('Firestore');
const getAllEventData = require('getAllEventData');
const getTimestampMillis = require('getTimestampMillis');
const log = require('logToConsole');
const JSON = require('JSON');
const Object = require('Object');


if(data.enable_logs){log('SERVER-SIDE GTM TAG: TAG SETTINGS');}

// Firestore project settings
const project_id = data.project_id;
const collection_path = data.collection_path;

if(data.enable_logs){log('TABLE INFOS');}

const timestamp = getTimestampMillis() / 1000;

const event = {};
const parameters = data.parameters;

for (let i=0; i < parameters.length; i++){
  const name = parameters[i].column_name;
  const value = parameters[i].column_value;
  event[name] = value;
}


if(data.add_timestamp){event[data.timestamp_param_name] = timestamp;}

if(data.enable_logs){log('👉 Payload to insert: ', event);}

// Write to Firestore
if(data.write_mode == 'new'){
  if(data.enable_logs){log('👉 Create new document in ' + collection_path);}
  sendToFirestore(project_id, collection_path, event);
  log('👉 New document created in ' + collection_path);

} else {
  if(data.enable_logs){log('👉 Update documents in ' + collection_path);}
  if(data.enable_logs){log('👉 Join keys:');}
  const queries = [];
  for (var i = 0; i < data.join_parameters.length; i++) {
    const key = data.join_parameters[i].merge_key;
    const operator = data.join_parameters[i].merge_operator;
    const value = data.join_parameters[i].merge_value;

    if(data.enable_logs){log('- ' + key + ': ' + value);}

    var query = [key, operator, value];
    queries.push(query);
  }

  Firestore.runTransaction((transaction) => {
    const transactionOptions = {
      projectId: project_id,
      transaction: transaction,
    };
    return Firestore.query(collection_path, queries, {projectId: project_id, limit: 0}).then((documents) => {
      if(documents.length === 0) {
        if(data.enable_logs)log('👉 No data inserted.');
      } else {
        for (var i=0; i < documents.length; i++){
          sendToFirestore(project_id, documents[i].id, event);
          if(data.enable_logs)log('👉 Data inserted in: ' + documents[i].id);
        }
      }
    });
  }, {
    projectId: project_id
  });
}

// Write to Firestore
function sendToFirestore(project_id, path, event){
  Firestore.write(path, event, {
    projectId: project_id,
    merge: data.merge,
  }).then((id) => data.gtmOnSuccess(), data.gtmOnFailure);
}
