import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.CategorySeries;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.AxesChartStyler;
import org.knowm.xchart.style.CategoryStyler;
import org.knowm.xchart.style.Styler;

import java.util.List;
import java.util.Locale;

public class CategoryBarChart {

    public static CategoryChart barChart(String name , String x_title, List<String> feature, List<Long> counts) {

        // Create Chart
        CategoryChart chart =
                new CategoryChartBuilder()
                        .width(900)
                        .height(600)
                        .title("chart")
                        .xAxisTitle(x_title)
                        .yAxisTitle("Counts")
                        .build();

        // Customize Chart
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        //chart.getStyler().setLabelsVisible(false);
        chart.getStyler().setPlotGridLinesVisible(false);

        chart.addSeries( name,feature, counts);

        return chart;
    }
}