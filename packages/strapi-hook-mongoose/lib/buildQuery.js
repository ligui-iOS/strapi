const _ = require('lodash');
const utils = require('./utils')();
const util = require('util');

const buildQuery = ({ model, filters, populate } = {}) => {
  let query = model.aggregate()

  if (_.has(filters, 'where') && Array.isArray(filters.where)) {
    const joins = buildJoinsFromWhere({ model, where: filters.where });

    const matches = filters.where.map(whereClause => {
      return {
        $match: buildWhereClause(whereClause),
      };
    });

    query = query.append([...joins, ...matches]);
  }

  if (Array.isArray(populate) && populate.length > 0) {
    const queryJoins = buildQueryJoins(model, { whitelistedPopulate: populate });
    // Remove redundant joins
    const differenceJoins = _.differenceWith(queryJoins, query.pipeline(), _.isEqual);
    query = query.append(differenceJoins);
  }

  if (_.has(filters, 'sort')) {
    const sortFilter = filters.sort.reduce((acc, sort) => {
      const { field, order } = sort;
      acc[field] = order === 'asc' ? 1 : -1;
      return acc;
    }, {});

    query = query.sort(sortFilter);
  }

  if (_.has(filters, 'start')) {
    query = query.skip(filters.start);
  }

  if (_.has(filters, 'limit' && filters.limit >= 0)) {
    query = query.limit(filters.limit);
  }

  return query;
};

const buildWhereClause = ({ field, operator, value }) => {
  switch (operator) {
    case 'eq':
      return { [field]: utils.valueToId(value) };
    case 'ne':
      return { [field]: { $ne: utils.valueToId(value) } };
    case 'lt':
      return { [field]: { $lt: value } };
    case 'lte':
      return { [field]: { $lte: value } };
    case 'gt':
      return { [field]: { $gt: value } };
    case 'gte':
      return { [field]: { $gte: value } };
    case 'in':
      return {
        [field]: {
          $in: Array.isArray(value) ? value.map(utils.valueToId) : [utils.valueToId(value)],
        },
      };
    case 'nin':
      return {
        [field]: {
          $nin: Array.isArray(value) ? value.map(utils.valueToId) : [utils.valueToId(value)],
        },
      };
    case 'contains': {
      return {
        [field]: {
          $regex: value,
          $options: 'i',
        },
      };
    }
    case 'ncontains':
      return {
        [field]: {
          $not: {
            $regex: value,
            $options: 'i',
          },
        },
      };
    case 'containss':
      return {
        [field]: {
          $regex: value,
        },
      };
    case 'ncontainss':
      return {
        [field]: {
          $not: {
            $regex: value,
          },
        },
      };

    default:
      throw new Error(`Unhandled whereClause : ${fullField} ${operator} ${value}`);
  }
};

const buildJoinsFromWhere = ({ model, where }) => {
  const relationToPopulate = extractRelationsFromWhere(where);
  let result = [];
  _.forEach(relationToPopulate, fieldPath => {
    const associationParts = fieldPath.split('.');

    let currentModel = model;
    let nextPrefixedPath = '';
    _.forEach(associationParts, astPart => {
      const association = currentModel.associations.find(a => a.alias === astPart);

      if (association) {
        const { models } = association.plugin ? strapi.plugins[association.plugin] : strapi;
        const model = models[association.collection || association.model];

        // Generate lookup for this relation
        result.push(
          ...buildQueryJoins(currentModel, {
            whitelistedPopulate: [astPart],
            prefixPath: nextPrefixedPath,
          })
        );

        currentModel = model;
        nextPrefixedPath += `${astPart}.`;
      }
    });
  });

  return result;
};

const extractRelationsFromWhere = where => {
  return where
    .map(({ field }) => {
      const parts = field.split('.');
      return parts.length === 1 ? field : _.initial(parts).join('.');
    })
    .sort()
    .reverse()
    .reduce((acc, currentValue) => {
      const alreadyPopulated = _.some(acc, item => _.startsWith(item, currentValue));
      if (!alreadyPopulated) {
        acc.push(currentValue);
      }
      return acc;
    }, []);
};

/**
 * Returns an array of relations to populate
 */
const buildQueryJoins = (strapiModel, { whitelistedPopulate = null, prefixPath = '' } = {}) => {
  return _.chain(strapiModel.associations)
    .filter(ast => {
      // Included only whitelisted relation if needed
      if (whitelistedPopulate) {
        return _.includes(whitelistedPopulate, ast.alias);
      }
      return ast.autoPopulate;
    })
    .map(ast => populateAssociation(ast, prefixPath))
    .flatten()
    .value();
};

const populateAssociation = (ast, prefixPath = '') => {
  const stages = [];
  const { models } = ast.plugin ? strapi.plugins[ast.plugin] : strapi;
  const model = models[ast.collection || ast.model];

  // Make sure that the model is defined (it'll not be defined in case of related association in upload plugin)
  if (!model) {
    return stages;
  }

  const from = model.collectionName;
  const isDominantAssociation = (ast.dominant && ast.nature === 'manyToMany') || !!ast.model;

  const _localField = !isDominantAssociation || ast.via === 'related' ? '_id' : ast.alias;

  const localField = `${prefixPath}${_localField}`;

  const foreignField = ast.filter ? `${ast.via}.ref` : isDominantAssociation ? '_id' : ast.via;

  // Add the juncture like the `.populate()` function
  const asTempPath = buildTempFieldPath(ast.alias, prefixPath);
  const asRealPath = restoreRealFieldPath(ast.alias, prefixPath);

  if (ast.plugin === 'upload') {
    // Filter on the correct upload field
    stages.push({
      $lookup: {
        from,
        let: { local_id: `$${localField}` },
        pipeline: [
          { $unwind: { path: `$${ast.via}`, preserveNullAndEmptyArrays: true } },
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: [`$${foreignField}`, '$$local_id'] },
                  { $eq: [`$${ast.via}.${ast.filter}`, ast.alias] },
                ],
              },
            },
          },
        ],
        as: asTempPath,
      },
    });
  } else {
    stages.push({
      $lookup: {
        from,
        localField,
        foreignField,
        as: asTempPath,
      },
    });
  }

  // Unwind the relation's result if only one is expected
  if (ast.type === 'model') {
    stages.push({
      $unwind: {
        path: `$${asTempPath}`,
        preserveNullAndEmptyArrays: true,
      },
    });
  }

  // Preserve relation field if it is empty
  stages.push({
    $addFields: {
      [asRealPath]: {
        $ifNull: [`$${asTempPath}`, null],
      },
    },
  });

  // Remove temp field
  stages.push({
    $project: {
      [asTempPath]: 0,
    },
  });

  return stages;
};

const buildTempFieldPath = field => {
  return `__${field}`;
};

const restoreRealFieldPath = (field, prefix) => {
  return `${prefix}${field}`;
};

module.exports = buildQuery;
