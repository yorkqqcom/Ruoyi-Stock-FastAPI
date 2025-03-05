import numpy as np
import pandas as pd
from scipy.fft import fft, fftfreq
from statsmodels.tsa.stattools import acf
import matplotlib.pyplot as plt
import pymysql
from sqlalchemy import create_engine


class StockCycleAnalyzer:
    def __init__(self, symbol='000001', adjust_type='hfq'):
        self.symbol = symbol
        self.adjust_type = adjust_type
        self.engine = create_engine('mysql+pymysql://rouyi:123456@127.0.0.1:3306/dash?charset=utf8mb4')

    def load_data(self):
        """从数据库加载历史数据"""

        query = f"""
        SELECT h.date, h.close, s.change_pct_2d, s.change_pct_3d, 
               s.change_pct_5d, s.change_pct_10d, h.amplitude
        FROM stock_hist h
        LEFT JOIN stock_hist_stats s   # 改为LEFT JOIN
          ON h.symbol = s.symbol 
         AND h.date = s.date 
         AND h.adjust_type = s.adjust_type
        WHERE h.symbol = '{self.symbol}' 
        ORDER BY h.date"""
        # print(query)
        df = pd.read_sql(query, self.engine)
        print(f"Loaded {len(df)} records for {self.symbol}")  # 新增调试语句
        return df


    def detect_cycles(self, series, max_period=60):
        # 处理空数据
        if len(series) < 2:
            return {'fourier': 0, 'acf': 0, 'final': 0}

        # 添加标准化处理
        normalized = (series - series.mean()) / series.std()

        # 傅里叶变换容错
        try:
            yf = fft(normalized.values)
        except:
            return {'fourier': 0, 'acf': 0, 'final': 0}
        """复合周期检测算法"""
        # 傅里叶变换分析
        n = len(series)
        yf = fft(series.values)
        xf = fftfreq(n, 1)[:n // 2]
        freqs = np.abs(yf[0:n // 2])
        top_freq = xf[np.argmax(freqs)]

        # 自相关分析
        lag_acf = acf(series, nlags=max_period)
        candidate_lags = np.where(lag_acf > 0.5)[0]

        # 动态周期整合
        fourier_period = int(1 / top_freq) if top_freq != 0 else 0
        acf_period = candidate_lags[-1] if len(candidate_lags) > 0 else 0

        return {
            'fourier': fourier_period,
            'acf': acf_period,
            'final': max(fourier_period, acf_period)
        }

    def analyze_cycles(self, window=120):
        df = self.load_data()
        # 动态调整窗口大小
        valid_window = min(window, len(df))
        if valid_window < 30:  # 设置最小分析窗口
            raise ValueError(f"数据不足（{len(df)}条），至少需要30条")

        # 修改循环逻辑
        results = []
        for i in range(len(df) - valid_window + 1):  # +1确保至少执行一次
            window_df = df.iloc[i:i + valid_window]

            # 确保返回值是字典且包含 'final' 键
            close_cycle = self.detect_cycles(window_df['close'])
            amplitude_cycle = self.detect_cycles(window_df['amplitude'])

            results.append({
                'start_date': window_df.iloc[0]['date'],
                'end_date': window_df.iloc[-1]['date'],
                'close_cycle': close_cycle,  # 确保键名正确
                'amplitude_cycle': amplitude_cycle,
                'resonance': close_cycle['final'] == amplitude_cycle['final']
            })

        cycle_df = pd.DataFrame(results)
        # 检查列是否存在
        if 'close_cycle' not in cycle_df.columns:
            raise KeyError("'close_cycle' 列未正确生成")
        return cycle_df

    def visualize(self, df, output_file=None):  # 添加参数定义
        if 'close_cycle' not in df.columns or 'amplitude_cycle' not in df.columns:
            raise KeyError("DataFrame 缺少周期分析列")

        # 确保列中所有元素为字典
        df['close_cycle'] = df['close_cycle'].apply(
            lambda x: x if isinstance(x, dict) else {'final': None}
        )
        df['amplitude_cycle'] = df['amplitude_cycle'].apply(
            lambda x: x if isinstance(x, dict) else {'final': None}
        )

        # 提取周期值
        df['close_cycle_final'] = df['close_cycle'].apply(lambda x: x.get('final'))
        df['amplitude_cycle_final'] = df['amplitude_cycle'].apply(lambda x: x.get('final'))

        # 可视化分析
        plt.figure(figsize=(15, 8))
        ax1 = plt.subplot(211)
        ax1.plot(df['end_date'], df['close_cycle_final'], label='Close Cycle')
        ax1.set_title(f'{self.symbol} Price Cycle Analysis')

        ax2 = plt.subplot(212)
        ax2.plot(df['end_date'], df['amplitude_cycle_final'], color='orange', label='Amplitude Cycle')
        ax2.set_title('Amplitude Cycle Analysis')

        plt.tight_layout()
        plt.show()

        # 文字结论输出
        conclusion = self.generate_conclusion(df)
        print("\n" + "#" * 40 + " 分析结论 " + "#" * 40)
        print(conclusion)

        # 文件输出（仅在output_file非空时执行）
        if output_file:  # 此时output_file已定义
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(conclusion)
                print(f"\n结论已保存至：{output_file}")

        # 日志记录
        import logging
        logging.basicConfig(filename='stock_analysis.log', level=logging.INFO)
        logging.info(f"\n{conclusion}")

    def generate_conclusion(self, df):
        """生成文字结论"""
        # 基础统计
        avg_close_cycle = df['close_cycle_final'].mean()
        resonance_rate = df['resonance'].mean() * 100
        max_cycle = df['close_cycle_final'].max()

        # 关键转折点检测（新增逻辑）
        cycle_changes = df['close_cycle_final'].diff().abs()
        major_changes = df[cycle_changes > 5]

        # 使用f-string格式化（参考网页1）
        conclusion = f"""
        * 周期分析报告（{self.symbol}）*
        --------------------------------------
        1️⃣ 平均价格周期：{avg_close_cycle:.1f}个交易日
        2️⃣ 共振现象频率：{resonance_rate:.1f}% 的窗口期出现价格/振幅周期同步
        3️⃣ 最大检测周期：{max_cycle}日（可能反映长期趋势）
    
        *关键转折点*：
        {len(major_changes)}次显著周期变化，最近发生在：
        {major_changes['end_date'].tail(3).values}
        """
        return conclusion
# 使用示例
if __name__ == "__main__":
    analyzer = StockCycleAnalyzer(symbol='000001')
    cycle_df = analyzer.analyze_cycles(window=120)
    analyzer.visualize(cycle_df)