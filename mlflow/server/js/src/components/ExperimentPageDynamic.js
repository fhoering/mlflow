import React, { Component } from 'react';
import './ExperimentPageDynamic.css';
import {
  wrapDeferred,
} from '../Actions'
import { withRouter } from 'react-router-dom';
import { MlflowService } from '../sdk/MlflowService'
import ReactTable from "react-table";
import Select from 'react-select';
import "react-table/react-table.css";
import chroma from 'chroma-js';

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
  cloned['end_time'] = new Date(parseInt(cloned['end_time'])).toLocaleString()
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


const colourStyles = {
  control: styles => ({ ...styles, backgroundColor: 'white' }),
  option: (styles, { data, isDisabled, isFocused, isSelected }) => {
    const color = chroma(data.color);
    console.log(data.color)
    return {
      ...styles,
      backgroundColor: isDisabled
        ? null
        : isSelected
        ? data.color
        : isFocused
        ? color.alpha(0.1).css()
        : null,
      color: isDisabled
        ? '#ccc'
        : isSelected
        ? chroma.contrast(color, 'white') > 2
          ? 'white'
          : 'black'
        : data.color,
      cursor: isDisabled ? 'not-allowed' : 'default',

      ':active': {
        ...styles[':active'],
        backgroundColor: !isDisabled && (isSelected ? data.color : color.alpha(0.3).css()),
      },
    };
  },
  multiValue: (styles, { data }) => {
    const color = chroma(data.color);
    return {
      ...styles,
      backgroundColor: color.alpha(0.1).css(),
    };
  },
  multiValueLabel: (styles, { data }) => ({
    ...styles,
    color: data.color,
  }),
  multiValueRemove: (styles, { data }) => ({
    ...styles,
    color: data.color,
    ':hover': {
      backgroundColor: data.color,
      color: 'white',
    },
  }),
};

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

const columnColors = {
  'metrics': 'blue',
  'tags': 'orange',
  'attributes': 'red',
  'params': 'green'
}

const toColumn = (prefix) => (name) => ({
  Header: name,
  id: prefix+'.`'+name+'`',
  value: prefix+'.`'+name+'`',
  accessor: d => d[prefix][name],
  kind: prefix,
  label: name,
  color: columnColors[prefix]
})

export class ExperimentPageDynamic extends Component {
  constructor(props) {
    super(props);
    const defaultColumns = [].concat([
        'start_time'
      ].map(toColumn('attributes')),
      [
        'mlflow.user',
        'mlflow.runName',
      ].map(toColumn('tags')),
      [
        'triplet_precision',
      ].map(toColumn('metrics')),
      [
        'batch_size'
      ].map(toColumn('params'))
    );
    this.state = {
      data: [],
      pages: null,
      loading: true,
      columns: defaultColumns,
      availableColumns: defaultColumns
    };
    this.fetchData = this.fetchData.bind(this);
    this.handleAttributesChange = this.handleAttributesChange.bind(this);
    this.fetchColumns()
  }

  fetchColumns() {
    const { experimentId } = this.props;
    wrapDeferred(MlflowService.listAllColumns, {
      experiment_ids: [experimentId]
    }).then(({attributes, tags, metrics, params}) => {
      const collator = new Intl.Collator('en');
      const columns = [].concat(
        attributes.sort(collator.compare).map(toColumn('attributes')),
        metrics.sort(collator.compare).map(toColumn('metrics')),
        params.sort(collator.compare).map(toColumn('params')),
        tags.sort(collator.compare).map(toColumn('tags')))

      this.setState({
        availableColumns: columns
      })
    })
  }

  extractColumnsPerKind(columns, kindSelected) {
    return columns.filter(({kind}) => kind === kindSelected).map(({label}) => label)
  }

  fetchData(state) {
    const { experimentId } = this.props;
    const { columns } = this.state;
    this.setState({ loading: true });
    requestData(
      experimentId,
      state.pageSize,
      state.page,
      state.sorted,
      state.filtered,
      this.extractColumnsPerKind(columns, 'attributes'),
      this.extractColumnsPerKind(columns, 'tags'),
      this.extractColumnsPerKind(columns, 'metrics'),
      this.extractColumnsPerKind(columns, 'params')
    ).then(res => {
      this.setState({
        data: res.rows,
        pages: res.pages,
        loading: false
      });
    });
  }

  handleAttributesChange(columns) {
    this.setState({columns: columns === null ? [] : columns})
  }

  render() {
    const { data, pages, loading, columns, availableColumns } = this.state;

    return (
      <div>
        <Select
          value={columns}
          onChange={this.handleAttributesChange}
          options={availableColumns}
          isMulti={true}
          isSearchable={true}
          styles={colourStyles}
        />
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
