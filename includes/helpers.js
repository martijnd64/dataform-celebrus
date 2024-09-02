const getNode = (
  nodeField,
  nodePath = "$.processing.graph.nodeDefinitions.matchNode",
) => {
  return `CASE
    WHEN JSON_VALUE(xml_string, '${nodePath}.${nodeField}') IS NOT NULL THEN ARRAY( SELECT JSON_VALUE(xml_string, '${nodePath}.${nodeField}'))
    ELSE (
  SELECT
    ARRAY_AGG(JSON_VALUE(nodeArr, '$.${nodeField}'))
  FROM
    UNNEST(JSON_QUERY_ARRAY(xml_string, '${nodePath}')) AS nodeArr)
  END
    AS node${nodeField.charAt(0).toUpperCase() + nodeField.slice(1)}`;
};

module.exports = { getNode };