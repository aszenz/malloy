/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import {
  compileModel,
  compileQuery,
  compileSource,
  extractSourceDependencies,
} from './stateless';
import * as Malloy from '@malloydata/malloy-interfaces';

describe('api', () => {
  describe('compile model', () => {
    test('compile model with table dependency', () => {
      const result = compileModel({
        model_url: 'file://test.malloy',
        compiler_needs: {
          table_schemas: [
            {
              connection_name: 'connection',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          files: [
            {
              url: 'file://test.malloy',
              contents: "source: flights is connection.table('flights')",
            },
          ],
          connections: [{name: 'connection', dialect: 'duckdb'}],
        },
      });
      const expected: Malloy.CompileModelResponse = {
        model: {
          entries: [
            {
              kind: 'source',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          anonymous_queries: [],
        },
      };
      expect(result).toMatchObject(expected);
    });
    test('compile model with model extension', () => {
      const result = compileModel({
        model_url: 'file://test.malloy',
        extend_model_url: 'file://base.malloy',
        compiler_needs: {
          table_schemas: [
            {
              connection_name: 'connection',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          files: [
            {
              url: 'file://base.malloy',
              contents: "source: flights_base is connection.table('flights')",
            },
            {
              url: 'file://test.malloy',
              contents: 'source: flights is flights_base',
            },
          ],
          connections: [{name: 'connection', dialect: 'duckdb'}],
        },
      });
      const expected: Malloy.CompileModelResponse = {
        model: {
          entries: [
            {
              kind: 'source',
              name: 'flights_base',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
            {
              kind: 'source',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          anonymous_queries: [],
        },
      };
      expect(result).toMatchObject(expected);
    });
    test('compile model with sql dependency', () => {
      const result = compileModel({
        model_url: 'file://test.malloy',
        compiler_needs: {
          sql_schemas: [
            {
              connection_name: 'connection',
              sql: 'SELECT 1 as one',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'one',
                    type: {kind: 'number_type'},
                  },
                ],
              },
            },
          ],
          files: [
            {
              url: 'file://test.malloy',
              contents: "source: flights is connection.sql('SELECT 1 as one')",
            },
          ],
          connections: [{name: 'connection', dialect: 'duckdb'}],
        },
      });
      const expected: Malloy.CompileModelResponse = {
        model: {
          entries: [
            {
              kind: 'source',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'one',
                    type: {kind: 'number_type'},
                  },
                ],
              },
            },
          ],
          anonymous_queries: [],
        },
      };
      expect(result).toMatchObject(expected);
    });
  });
  test('compile model with turducken sql dependency', () => {
    const sql =
      '\n                SELECT carrier FROM (SELECT \n   base."carrier" as "carrier"\nFROM flights as base\nGROUP BY 1\nORDER BY 1 asc NULLS LAST\n)\n              ';
    const result = compileModel({
      model_url: 'file://test.malloy',
      compiler_needs: {
        table_schemas: [
          {
            connection_name: 'connection',
            name: 'flights',
            schema: {
              fields: [
                {
                  kind: 'dimension',
                  name: 'carrier',
                  type: {kind: 'string_type'},
                },
              ],
            },
          },
        ],
        sql_schemas: [
          {
            connection_name: 'connection',
            sql,
            schema: {
              fields: [
                {
                  kind: 'dimension',
                  name: 'carrier',
                  type: {kind: 'string_type'},
                },
              ],
            },
          },
        ],
        files: [
          {
            url: 'file://test.malloy',
            contents: `
              source: flights is connection.table('flights')
              source: sql_source is connection.sql("""
                SELECT carrier FROM (%{
                  flights -> { group_by: carrier }
                })
              """)
            `,
          },
        ],
        connections: [{name: 'connection', dialect: 'duckdb'}],
      },
    });
    const expected: Malloy.CompileModelResponse = {
      model: {
        entries: [
          {
            kind: 'source',
            name: 'flights',
            schema: {
              fields: [
                {
                  kind: 'dimension',
                  name: 'carrier',
                  type: {kind: 'string_type'},
                },
              ],
            },
          },
          {
            kind: 'source',
            name: 'sql_source',
            schema: {
              fields: [
                {
                  kind: 'dimension',
                  name: 'carrier',
                  type: {kind: 'string_type'},
                },
              ],
            },
          },
        ],
        anonymous_queries: [],
      },
    };
    expect(result).toMatchObject(expected);
  });
  describe('compile query', () => {
    test('compile query with table dependency', () => {
      const result = compileQuery({
        model_url: 'file://test.malloy',
        query: {
          definition: {
            kind: 'arrow',
            source_reference: {name: 'flights'},
            view: {
              kind: 'segment',
              operations: [
                {
                  kind: 'group_by',
                  field: {
                    expression: {kind: 'field_reference', name: 'carrier'},
                  },
                },
              ],
            },
          },
        },
        compiler_needs: {
          table_schemas: [
            {
              connection_name: 'connection',
              name: 'flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          files: [
            {
              url: 'file://test.malloy',
              contents: "source: flights is connection.table('flights')",
            },
          ],
          connections: [{name: 'connection', dialect: 'duckdb'}],
        },
      });
      const expected: Malloy.CompileQueryResponse = {
        result: {
          connection_name: 'connection',
          sql: `SELECT \n\
   base."carrier" as "carrier"
FROM flights as base
GROUP BY 1
ORDER BY 1 asc NULLS LAST
`,
          schema: {
            fields: [
              {
                kind: 'dimension',
                name: 'carrier',
                type: {kind: 'string_type'},
              },
            ],
          },
        },
      };
      expect(result).toMatchObject(expected);
    });
  });
  describe('compiler errors', () => {
    test('parse error should come back as a log', () => {
      const result = compileModel({
        model_url: 'file://test.malloy',
        compiler_needs: {
          files: [
            {
              url: 'file://test.malloy',
              contents: 'run: flights -> { group_by: carrier }',
            },
          ],
        },
      });
      const expected: Malloy.CompileModelResponse = {
        logs: [
          {
            url: 'file://test.malloy',
            severity: 'error',
            message: "Reference to undefined object 'flights'",
            range: {
              start: {line: 0, character: 5},
              end: {line: 0, character: 12},
            },
          },
        ],
      };
      expect(result).toMatchObject(expected);
    });
    test('missing source should come back as a log', () => {
      const result = compileSource({
        model_url: 'file://test.malloy',
        name: 'flights',
        compiler_needs: {
          files: [
            {
              url: 'file://test.malloy',
              contents: '// nothing to see here',
            },
          ],
        },
      });
      const expected: Malloy.CompileModelResponse = {
        logs: [
          {
            url: 'file://test.malloy',
            severity: 'error',
            message: 'Model does not contain a source named flights',
            range: {
              start: {line: 0, character: 0},
              end: {line: 0, character: 0},
            },
          },
        ],
      };
      expect(result).toMatchObject(expected);
    });
  });
  describe('extract sql artifact dependencies from a source', () => {
    test('source with a single table dependency', () => {
      const flightsTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'flights',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'origin',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'destination',
              type: {kind: 'string_type'},
            },
          ],
        },
      };

      const result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'flights',
        compiler_needs: {
          table_schemas: [flightsTable],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                source: flights is connection.table('flights')
              `,
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
        },
      });
      const expected: Malloy.ExtractSourceDependenciesResponse = {
        sql_sources: [flightsTable],
      };

      expect(result).toMatchObject(expected);
    });
    test('source with a sql query dependency', () => {
      const _result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'sql_source',
        compiler_needs: {
          sql_schemas: [
            {
              connection_name: 'connection',
              sql: 'SELECT carrier FROM flights',
              schema: {
                fields: [
                  {
                    kind: 'dimension',
                    name: 'carrier',
                    type: {kind: 'string_type'},
                  },
                ],
              },
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                source: sql_source is connection.sql('SELECT carrier FROM flights')
              `,
            },
          ],
        },
      });
      // const expected: Malloy.ExtractSourceDependenciesResponse = {
      //   sql_sources: [{sql, connection_name: 'connection'}],
      // };

      // expect(result).toMatchObject(expected);
    });
    test('source extends another source', () => {
      const flightsTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'flights',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'origin',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'destination',
              type: {kind: 'string_type'},
            },
          ],
        },
      };

      const _result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'flights',
        compiler_needs: {
          table_schemas: [flightsTable],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                ##! experimental.access_modifiers
                source: flights is connection.table('flights')
                source: pared_flights is flights include {
                  except: destination
                }
              `,
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
        },
      });
    });
    test('source with joins', () => {
      const flightsTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'flights',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'origin',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'destination',
              type: {kind: 'string_type'},
            },
          ],
        },
      };
      const carriersTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'carriers',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'year_founded',
              type: {kind: 'number_type'},
            },
          ],
        },
      };

      const _result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'flights',
        compiler_needs: {
          table_schemas: [flightsTable, carriersTable],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                source: flights is connection.table('flights')
                source: carriers is connection.table('carriers')

                source: flights_with_carrier_dim is flights extend {
                  join_many: carriers on carrier = carriers.carrier
                }
              `,
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
        },
      });
    });
    test('source with pipeline', () => {
      const flightsTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'flights',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'origin',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'destination',
              type: {kind: 'string_type'},
            },
          ],
        },
      };

      const _result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'derived',
        compiler_needs: {
          table_schemas: [flightsTable],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                source: flights is connection.table('flights')

                source: derived is flights -> {select: origin, destination} extend {
                  dimension: trip is concat(origin, '-', destination)
                }
              `,
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
        },
      });
    });
    test('source with filters', () => {
      const flightsTable: Malloy.SQLTable = {
        connection_name: 'connection',
        name: 'flights',
        schema: {
          fields: [
            {
              kind: 'dimension',
              name: 'carrier',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'origin',
              type: {kind: 'string_type'},
            },
            {
              kind: 'dimension',
              name: 'destination',
              type: {kind: 'string_type'},
            },
          ],
        },
      };

      const _result = extractSourceDependencies({
        model_url: 'file://test.malloy',
        source_name: 'derived',
        compiler_needs: {
          table_schemas: [flightsTable],
          files: [
            {
              url: 'file://test.malloy',
              contents: `
                source: flights is connection.table('flights') extend {
                  where: carrier ? 'UA' | 'AA'
                }
              `,
            },
          ],
          connections: [{name: 'connection', dialect: 'presto'}],
        },
      });
    });
    test('composite source', () => {});
  });
});
