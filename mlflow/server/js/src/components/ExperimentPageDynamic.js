import React, { Component } from 'react';
import './ExperimentPageDynamic.css';
import {
  wrapDeferred,
} from '../Actions'
import { withRouter } from 'react-router-dom';
import { MlflowService } from '../sdk/MlflowService'
import ReactTable from "react-table";
import "react-table/react-table.css";

function keyValueListToObject(l) {
  if(l === undefined) {
    return {}
  }
  return l.reduce((acc,{key, value}) => {
    acc[key] = value;
    return acc;
  },{})
}

function transformAttributesValues(attributes) {
  const cloned = {...attributes};
  cloned['start_time'] = new Date(parseInt(cloned['start_time'])).toLocaleString()
  return cloned;
}

function transformRun(r) {
  return {
    attributes: transformAttributesValues(r.info),
    tags: r.data === undefined ? {} : keyValueListToObject(r.data.tags),
    metrics: r.data === undefined ? {} : keyValueListToObject(r.data.metrics),
    params: r.data === undefined ? {} : keyValueListToObject(r.data.params)
  }
}

const requestData = (experimentId, pageSize, page, sorted, filtered,
  columnsAttributes, columnsTags, columnsMetrics, columnsParams) => {
  const orderBy = sorted.map(({id, desc}) => id+(desc ? ' DESC' : ''))
  const filter = filtered.map(({id, value}) => `${id} = "${value}"`).join(' ')

  return wrapDeferred(MlflowService.searchRuns, {
    experiment_ids: [experimentId],
    max_results: pageSize,
    page_token: page> 0 ? window.btoa(JSON.stringify({offset: page*pageSize})) : undefined,
    order_by: orderBy,
    tags_whitelist: {fields: columnsTags},
    metrics_whitelist: {fields: columnsMetrics},
    params_whitelist: {fields: columnsParams},
    filter
  }).then(a => {
    if (a.runs === undefined) {
      return {
        rows: [],
        pages: 1
      }
    }
    console.log(a.runs.map(transformRun))
    return {
      rows: a.runs.map(transformRun),
      pages: Math.ceil(a['total_run_count'] / pageSize)
    }
  })
};

export class ExperimentPageDynamic extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: [],
      pages: null,
      loading: true,
      columnsAttributes: ['start_time'],
      columnsTags: ['mlflow.user', 'mlflow.runName'],
      columnsMetrics: ['triplet_precision'],
      columnsParams: ['batch_size']
    };
    this.fetchData = this.fetchData.bind(this);
  }

  fetchData(state) {
    const { experimentId } = this.props;
    const { columnsAttributes, columnsTags, columnsMetrics, columnsParams } = this.state;
    this.setState({ loading: true });
    requestData(
      experimentId,
      state.pageSize,
      state.page,
      state.sorted,
      state.filtered,
      columnsAttributes,
      columnsTags,
      columnsMetrics,
      columnsParams
    ).then(res => {
      this.setState({
        data: res.rows,
        pages: res.pages,
        loading: false
      });
    });
  }

  render() {
    const { data, pages, loading, columnsAttributes, columnsTags, columnsMetrics, columnsParams } = this.state;
    const toColumn = (prefix) => (name) => ({
      Header: name,
      id: prefix+'.`'+name+'`',
      accessor: d => d[prefix][name]
    })
    const columns = [].concat(
      columnsAttributes.map(toColumn('attributes')),
      columnsTags.map(toColumn('tags')),
      columnsMetrics.map(toColumn('metrics')),
      columnsParams.map(toColumn('params')))
    return (
      <div>
        <ReactTable
          columns={columns}
          manual // Forces table not to paginate or sort automatically, so we can handle it server-side
          data={data}
          pages={pages} // Display the total number of pages
          loading={loading} // Display the loading overlay when we need it
          onFetchData={this.fetchData} // Request new data when things change
          filterable
          defaultPageSize={10}
          className="-striped -highlight"
        />
      </div>
    );
  }
}


export default withRouter(ExperimentPageDynamic);
