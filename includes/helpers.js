/*
SELECT
    "${constants.XML_AVERO_ACHMEA}" AS config,
    cb_functions.XML_TO_JSON(raw_data) as xml_string
  FROM
    ${ref(constants.XML_PREFIX + constants.XML_AVERO_ACHMEA)}
  UNION ALL
  SELECT
    "${constants.XML_CB_KLANTDOMEIN}" AS config,
    cb_functions.XML_TO_JSON(raw_data) as xml_string
  FROM
    ${ref(constants.XML_PREFIX + constants.XML_CB_KLANTDOMEIN)}
*/

const getXmlData = (
  name,
  includeUnion = true,
) => {
  const addUnion = includeUnion ? 'UNION ALL' : '';
  return `${addUnion} 
  SELECT
    "${constants["XML_"+name]}" AS config,
    cb_functions.XML_TO_JSON(raw_data) as xml_string
  FROM
    ${constants.DATABASE}.${constants.DATASET}.${constants.XML_PREFIX + constants["XML_"+name]}`;
};

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

const getNestedNode = (
    nodeField,
    nodePathPart,
    nodePathBase = "$.processing.graph.nodeDefinitions.matchNode",
) => {
    return `CASE
    WHEN JSON_VALUE(xml_string, '${nodePathBase}.${nodePathPart}.${nodeField}') IS NOT NULL THEN ARRAY( SELECT JSON_VALUE(xml_string, '${nodePathBase}.${nodePathPart}.${nodeField}'))
    WHEN JSON_VALUE(xml_string, '${nodePathBase}.${nodePathPart}[0].${nodeField}') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(JSON_VALUE(nodeArr, '$.${nodeField}'))
  FROM
    UNNEST(JSON_QUERY_ARRAY(xml_string, '${nodePathBase}.${nodePathPart}')) AS nodeArr)
    WHEN JSON_VALUE(xml_string, '${nodePathBase}[0].${nodePathPart}.${nodeField}') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(IFNULL(JSON_VALUE(nodeArr, '$.${nodePathPart}.${nodeField}'), ''))
  FROM
    UNNEST(JSON_QUERY_ARRAY(xml_string, '${nodePathBase}')) AS nodeArr)
    WHEN JSON_VALUE(xml_string, '${nodePathBase}[0].${nodePathPart}[0].${nodeField}') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(IFNULL(JSON_VALUE(nodeArr, '$.${nodeField}'), ''))
  FROM
    UNNEST(
      (SELECT
        JSON_QUERY_ARRAY(JSON_QUERY(xml_string, '${nodePathBase}'), '$[0].${nodePathPart}'))
    ) AS nodeArr)
  END
    AS node${nodeField.charAt(0).toUpperCase() + nodeField.slice(1)}`;
};

/*
  CASE
    WHEN JSON_VALUE(xml_string, '$.processing.graph.nodeDefinitions.matchNode.match.constraints.valueconstraint.attributePathName') IS NOT NULL THEN ARRAY( SELECT JSON_VALUE(xml_string, '$.processing.graph.nodeDefinitions.matchNode.match.constraints.valueconstraint.attributePathName'))
    WHEN JSON_VALUE(xml_string, '$.processing.graph.nodeDefinitions.matchNode.match.constraints.valueconstraint[0].attributePathName') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(JSON_VALUE(nodeArr, '$.attributePathName'))
  FROM
    UNNEST(JSON_QUERY_ARRAY(xml_string, '$.processing.graph.nodeDefinitions.matchNode.match.constraints.valueconstraint')) AS nodeArr)
    WHEN JSON_VALUE(xml_string, '$.processing.graph.nodeDefinitions.matchNode[0].match.constraints.valueconstraint.attributePathName') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(IFNULL(JSON_VALUE(nodeArr, '$.match.constraints.valueconstraint.attributePathName'), ''))
  FROM
    UNNEST(JSON_QUERY_ARRAY(xml_string, '$.processing.graph.nodeDefinitions.matchNode')) AS nodeArr)
    WHEN JSON_VALUE(xml_string, '$.processing.graph.nodeDefinitions.matchNode[0].match.constraints.valueconstraint[0].attributePathName') IS NOT NULL THEN (
  SELECT
    ARRAY_AGG(IFNULL(JSON_VALUE(nodeArr, '$.attributePathName'), ''))
  FROM
    UNNEST(
      (SELECT
        JSON_QUERY_ARRAY(JSON_QUERY(xml_string, '$.processing.graph.nodeDefinitions.matchNode'), '$[0].match.constraints.valueconstraint'))
    ) AS nodeArr)
  END
    AS nodeMatchAttribute,
*/

module.exports = { getXmlData, getNode, getNestedNode };