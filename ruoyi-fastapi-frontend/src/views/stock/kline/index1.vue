<template>
  <div class="app-container">
    <!-- 查询条件 -->
    <el-form :inline="true">
      <el-form-item label="股票代码">
        <el-input
          v-model="queryForm.symbol"
          placeholder="如：600000.SH"
          class="dark-input"
          style="width: 200px"
        />
      </el-form-item>

      <el-form-item label="复权类型">
        <el-select
          v-model="queryForm.adjustType"
          class="dark-select"
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
          class="dark-date-picker"
          value-format="yyyy-MM-dd"
          range-separator="至"
        />
      </el-form-item>

      <el-button
        type="primary"
        @click="loadData"
        :loading="loading"
        icon="el-icon-search"
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
  return data.map((_, index) => {
    if (index < window - 1) return null
    const sum = data
      .slice(index - window + 1, index + 1)
      .reduce((a, b) => a + b[2], 0)
    return sum / window
  })
}

export default {
  data() {
    const end = new Date()
    const start = new Date()
    start.setMonth(start.getMonth() - 1)

    return {
      loading: false,
      queryForm: {
        symbol: '600000.SH',
        adjustType: 'qfq',
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
  },

  beforeDestroy() {
    window.removeEventListener('resize', this.handleResize)
    if (this.chartInstance) {
      this.chartInstance.dispose()
    }
  },

  methods: {
    // 初始化图表
    initChart() {
      this.chartInstance = echarts.init(this.$refs.chart)
      const option = {
        backgroundColor: '#1a1a1a',
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'cross' }
        },
        legend: {
          data: ['K线', '成交量', 'MA5', 'MA10'],
          textStyle: { color: '#ccc' }
        },
        grid: [
          { left: '10%', right: '8%', height: '60%' },  // K线图区域
          { left: '10%', right: '8%', top: '72%', height: '15%' } // 成交量区域
        ],
        xAxis: [
          {
            type: 'time',
            boundaryGap: false,
            axisLine: { lineStyle: { color: '#4a4a4a' } },
            axisLabel: { color: '#999' },
            splitLine: { show: false }
          },
          {
            type: 'time',
            gridIndex: 1,
            axisLine: { lineStyle: { color: '#4a4a4a' } },
            axisLabel: { show: false },
            splitLine: { show: false }
          }
        ],
        yAxis: [
          {
            scale: true,
            splitNumber: 2,
            axisLine: { lineStyle: { color: '#4a4a4a' } },
            axisLabel: { color: '#999' },
            splitLine: { lineStyle: { type: 'dashed', color: '#333' } }
          },
          {
            scale: true,
            gridIndex: 1,
            splitNumber: 2,
            axisLine: { show: false },
            axisLabel: { show: false },
            splitLine: { show: false }
          }
        ],
        dataZoom: [
          {
            type: 'inside',
            xAxisIndex: [0, 1],
            start: 0,
            end: 100,
            minValueSpan: 3600 * 24 * 1000 * 7 // 最小缩放7天
          },
          {
            type: 'slider',
            xAxisIndex: [0, 1],
            bottom: 20,
            height: 18,
            handleStyle: { color: '#666' },
            textStyle: { color: '#999' },
            fillerColor: 'rgba(100, 100, 100, 0.2)'
          }
        ],
        series: [
          {
            name: 'K线',
            type: 'candlestick',
            itemStyle: {
              color: '#ef5350',
              color0: '#26a69a',
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
            itemStyle: {
              color: '#666'
            },
            data: []
          },
          {
            name: 'MA5',
            type: 'line',
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 1, color: '#ff9800' },
            data: []
          },
          {
            name: 'MA10',
            type: 'line',
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 1, color: '#2196f3' },
            data: []
          }
        ]
      }
      this.chartInstance.setOption(option)
    },

    // 加载数据
    async loadData() {
      this.loading = true
      try {
        const {data} = await getKline(this.queryForm)
        if (!data || data.length === 0) {
          this.$message.warning('暂无数据')
          return
        }

        // 处理原始数据
        const kData = data.map(d => [d.date, d.open, d.close, d.low, d.high])
        const volumes = data.map((d, index) => [d.date, d.volume, {
          value: d.close > d.open ? 1 : -1
        }])

        // 计算均线
        const closes = data.map(d => d.close)
        const ma5 = calculateMA(data, 5)
        const ma10 = calculateMA(data, 10)

        const option = {
          series: [
            {data: kData},
            {data: volumes},
            {data: ma5},
            {data: ma10}
          ]
        }
        this.chartInstance.setOption(option)
      } catch (error) {
        this.$message.error('数据加载失败')
      } finally {
        this.loading = false
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
    }
  }
}
</script>

<style scoped>
.app-container {
  background-color: #1a1a1a;
  padding: 20px;
  min-height: 100vh;
}

.chart-container {
  height: 700px;
  background: #1a1a1a;
  border-radius: 8px;
  margin-top: 20px;
}

::v-deep .el-form-item__label {
  color: #fff !important;
}

::v-deep .dark-input .el-input__inner {
  background-color: #333 !important;
  border-color: #666 !important;
  color: #fff !important;
}

::v-deep .dark-select .el-input__inner {
  background-color: #333 !important;
  border-color: #666 !important;
  color: #fff !important;
}

::v-deep .dark-date-picker .el-range-input {
  background-color: #333 !important;
  color: #fff !important;
}

::v-deep .dark-date-picker .el-range-separator {
  color: #fff !important;
}
</style>
