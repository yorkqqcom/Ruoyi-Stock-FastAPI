<template>
  <div class="app-container" @keydown.enter="handleEnterKey">
    <!-- 查询条件 -->
    <el-form :inline="true">
      <el-form-item label="股票代码">
        <el-input
          v-model="queryForm.symbol"
          :placeholder="lastSymbol || '如：600000'"
          class="light-input"
          style="width: 200px"
        />
      </el-form-item>

      <el-form-item label="复权类型">
        <el-select
          v-model="queryForm.adjustType"
          class="light-select"
        >
          <el-option label="前复权" value="qfq"/>
          <el-option label="后复权" value="hfq"/>
          <el-option label="不复权" value="normal"/>
        </el-select>
      </el-form-item>

      <el-form-item label="日期范围">
        <el-date-picker
          v-model="queryForm.dateRange"
          type="daterange"
          class="light-date-picker"
          value-format="yyyy-MM-dd"
          range-separator="至"
        />
      </el-form-item>

      <el-button
        type="primary"
        @click="loadData"
        :loading="loading"
        icon="el-icon-search"
        style="background: #409EFF;"
      >
        查询
      </el-button>
    </el-form>

    <!-- ECharts容器 -->
    <div ref="chart" class="chart-container"></div>
  </div>
</template>

<script>
import * as echarts from 'echarts'
import { getKline } from '@/api/stock/kline'

// 计算移动平均线
function calculateMA(data, window) {
  return data.map((d, index) => {
    if (index < window - 1) return null
    const sum = data
      .slice(index - window + 1, index + 1)
      .reduce((a, b) => a + b.close, 0)
    return Number((sum / window).toFixed(2))
  })
}

export default {
  data() {
    const end = new Date()
    const start = new Date()
    start.setMonth(start.getMonth() - 3 )

    return {
      lastSymbol: '',
      loading: false,
      queryForm: {
        symbol: '600000',
        adjustType: 'hfq',
        dateRange: [
          this.formatDate(start),
          this.formatDate(end)
        ]
      },
      chartInstance: null
    }
  },

  mounted() {
    this.initChart()
    this.loadData()
    window.addEventListener('resize', this.handleResize)
    window.addEventListener('keydown', this.handleKeyDown); // 新增
  },

  beforeDestroy() {
    window.removeEventListener('resize', this.handleResize)
    window.removeEventListener('keydown', this.handleKeyDown); // 新增
    if (this.chartInstance) {
      this.chartInstance.dispose()
    }
  },

  methods: {
    // 初始化图表
    initChart() {
      this.chartInstance = echarts.init(this.$refs.chart)
      const option = {
        backgroundColor: '#fff',
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'line' },
          backgroundColor: '#fff',
          borderColor: '#e4e7ed',
          textStyle: { color: '#303133' },
          formatter: function(params) {
            const kData = params.find(item => item.seriesType === 'candlestick')
            const ma5Item = params.find(item => item.seriesName === 'MA5')
            const ma10Item = params.find(item => item.seriesName === 'MA10')
            if (kData) {
            const [index,open, close, lowest, highest,volume] = kData.value
            return `
                ${echarts.time.format(kData.axisValue, '{yyyy}-{MM}-{dd}')}<br/>
                开盘价：${open.toFixed(2)}<br/>
                收盘价：${close.toFixed(2)}<br/>
                最低价：${lowest.toFixed(2)}<br/>
                最高价：${highest.toFixed(2)}<br/>
                成交量：${volume.toFixed(0)}<br/>
                ${ma5Item?.value !== null && !isNaN(ma5Item.value) ? `MA5: ${ma5Item.value.toFixed(2)}<br/>` : ''}
                ${ma10Item?.value !== null && !isNaN(ma10Item.value) ? `MA10: ${ma10Item.value.toFixed(2)}` : ''}
                `
                }
                return ''
                }
                },
                legend: {
                  data: ['K线', '成交量', 'MA5', 'MA10'],
                  textStyle: {color: '#606266'},
                  itemGap: 20
                },
                grid: [
                {left: '10%', right: '8%', height: '60%', top: '10%'},
                {left: '10%', right: '8%', top: '72%', height: '15%'}
                ],
                xAxis: [
                {
                  type: 'time',
                  boundaryGap: false,
                  axisLine: {lineStyle: {color: '#dcdfe6'}},
                  axisLabel: {color: '#909399'},
                  splitLine: {show: false},
                  formatter: function(value) {
                    // 显示纯日期格式
                    return echarts.time.format(value, '{yyyy}-{MM}-{dd}', false);
                    }
                },
                {
                  type: 'time',
                  gridIndex: 1,
                  axisLine: {lineStyle: {color: '#dcdfe6'}},

                  formatter: function(value) {
                    // 显示纯日期格式
                    return echarts.time.format(value, '{yyyy}-{MM}-{dd}', false);
                  },
                  axisLabel: {show: false},
                  splitLine: {show: false},
                }
                ],
                yAxis: [
                {
                  scale: true,
                  splitNumber: 2,
                  axisLine: {lineStyle: {color: '#dcdfe6'}},
                  axisLabel: {color: '#909399'},
                  splitLine: {lineStyle: {type: 'dashed', color: '#ebeef5'}}
                },
                {
                  scale: true,
                  gridIndex: 1,
                  splitNumber: 2,
                  axisLine: {show: false},
                  axisLabel: {show: false},
                  splitLine: {show: false}
                }
                ],
                dataZoom: [
                {
                  type: 'inside',
                  xAxisIndex: [0, 1],
                  start: 0,
                  end: 100,
                  minValueSpan: 3600 * 24 * 1000 * 7
                },
                {
                  type: 'slider',
                  xAxisIndex: [0, 1],
                  bottom: 20,
                  height: 18,
                  handleStyle: {color: '#909399'},
                  textStyle: {color: '#909399'},
                  fillerColor: 'rgba(144, 147, 153, 0.1)'
                }
                ],
                series: [
                {
                  name: 'K线',
                  type: 'candlestick',
                  itemStyle: {
                  color: '#ef5350',  // 跌色
                  color0: '#26a69a', // 涨色
                  borderColor: '#ef5350',
                  borderColor0: '#26a69a'
                },
                  data: []
                },
                {
                  name: '成交量',
                  type: 'bar',
                  xAxisIndex: 1,
                  yAxisIndex: 1,
                  // itemStyle: {
                  //   color: function(params) {
                  //     return params.data[2].value > 0 ? '#26a69a' : '#ef5350'
                  //   }
                  // },
                  data: []
                },
                {
                  name: 'MA5',
                  type: 'line',
                  smooth: true,
                  showSymbol: false,
                  lineStyle: {width: 2, color: '#ff9800'},
                  data: []
                },
                {
                  name: 'MA10',
                  type: 'line',
                  smooth: true,
                  showSymbol: false,
                  lineStyle: {width: 2, color: '#2196f3'},
                  data: []
                }
                ]
                }
                this.chartInstance.setOption(option)
                },

    // 加载数据
    // 在loadData方法中
    async loadData() {

      const currentSymbol = this.queryForm.symbol.trim()

      // 输入验证
      if (!/^\d{6}$/.test(currentSymbol)) {
        this.$message.warning('请输入6位数字股票代码')
        return
      }
      this.loading = true;
      try {
        const {data} = await getKline(this.queryForm);
        this.lastSymbol = currentSymbol
        // 清空股票代码输入框（仅在查询成功后）
        this.$nextTick(() => {
          this.queryForm.symbol = ''
          this.$refs.symbolInput.focus()
        })
        if (!data || data.length === 0) {
        this.$message.warning('暂无数据');
        return;
      }

      // 处理原始数据
      const dates = data.map(d => d.date);
      // 在 loadData 方法中处理数据时
      // const dates = data.map(d => {
      //   // 将日期字符串转换为时间戳（假设数据中的date字段是'2024-12-17'格式）
      //   return echarts.time.parse(d.date, '{yyyy}-{MM}-{dd}');
      // });
      // const kData = data.map(d => [d.open, d.close, d.low, d.high]);
      const kData = data.map(d => [d.open, d.close, d.low, d.high,d.volume]); // [开盘，收盘，最低，最高]
      // const volumes = data.map(d => [d.volume, d.close > d.open]);
      // const volumes = data.map(d => [d.volume, d.close > d.open]);
      const volumes = data.map(d => ({
      value: d.volume,
      itemStyle: {
      color: d.close > d.open ? '#26a69a' : '#ef5350'
    }
    }));
      // 计算均线
      const ma5 = calculateMA(data, 5);
      const ma10 = calculateMA(data, 10);

      // 更新图表配置
      const option = this.chartInstance.getOption();

      // 更新xAxis配置
      option.xAxis[0].type = 'category';
      option.xAxis[0].data = dates;
      option.xAxis[1].type = 'category';
      option.xAxis[1].data = dates;

      // 更新系列数据
      option.series[0].data = kData;
      option.series[1].data = volumes;
      option.series[2].data = ma5;
      option.series[3].data = ma10;

      // 调整数据缩放
      option.dataZoom[0].minValueSpan = 5;

      this.chartInstance.setOption(option);
    } catch (error) {
        this.queryForm.symbol = currentSymbol
        this.$message.error('数据加载失败');
    } finally {
      this.loading = false;
    }
    },

    handleResize() {
      this.chartInstance.resize()
    },

    formatDate(date) {
      const year = date.getFullYear()
      const month = String(date.getMonth() + 1).padStart(2, '0')
      const day = String(date.getDate()).padStart(2, '0')
      return `${year}-${month}-${day}`
    },
    handleKeyDown(event) {
      // 如果焦点在输入元素内，则不处理
      const activeElement = document.activeElement;
      const isInput = activeElement.tagName === 'INPUT' || activeElement.tagName === 'SELECT' || activeElement.tagName === 'TEXTAREA';
      if (isInput) {
        return;
      }

      const key = event.key;

      // 处理数字键
      if (key >= '0' && key <= '9') {
        event.preventDefault();
        this.queryForm.symbol += key;
      }

      // 处理退格键（可选）
      if (key === 'Backspace') {
        event.preventDefault();
        this.queryForm.symbol = this.queryForm.symbol.slice(0, -1);
      }

      // 处理回车键
      if (key === 'Enter') {
        event.preventDefault();
        if (this.queryForm.symbol) {
          this.loadData();
        }
      }
    },
    handleEnterKey(event) {
      // 排除以下情况：
      // 1. 正在输入其他表单元素
      // 2. 没有输入股票代码
      // 3. 正在加载中
      if (
        event.target.tagName === 'INPUT' ||
        event.target.tagName === 'TEXTAREA' ||
        !this.queryForm.symbol ||
        this.loading
      ) return

      this.loadData()
    },

    // 修改原有键盘处理（移除回车处理）
    handleKeyInput(event) {
      const activeTag = document.activeElement.tagName.toLowerCase()
      if (['input', 'textarea'].includes(activeTag)) return

      if (event.key >= '0' && event.key <= '9') {
        if (this.queryForm.symbol.length < 6) {
          this.queryForm.symbol += event.key
        }
        event.preventDefault()
      }

      if (event.key === 'Backspace') {
        this.queryForm.symbol = this.queryForm.symbol.slice(0, -1)
        event.preventDefault()
      }
    },
  }
}
</script>

<style scoped>
.app-container {
  background-color: #fff;
  padding: 20px;
  min-height: 100vh;
}

.chart-container {
  height: 700px;
  background: #fff;
  border: 1px solid #e4e7ed;
  border-radius: 4px;
  margin-top: 20px;
}

::v-deep .el-form-item__label {
  color: #606266 !important;
}

::v-deep .light-input .el-input__inner {
  background-color: #fff !important;
  border-color: #dcdfe6 !important;
  color: #303133 !important;
}

::v-deep .light-select .el-input__inner {
  background-color: #fff !important;
  border-color: #dcdfe6 !important;
  color: #303133 !important;
}

::v-deep .light-date-picker .el-range-input {
  background-color: #fff !important;
  color: #303133 !important;
}

::v-deep .light-date-picker .el-range-separator {
  color: #303133 !important;
}
</style>
