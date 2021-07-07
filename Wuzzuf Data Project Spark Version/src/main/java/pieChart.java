import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.style.Styler;

import java.util.List;

public class pieChart{

public static PieChart getPieChartforCompany(List<String> feature, List<String> count ) {

        // Create Chart
        PieChart chart =
        new PieChartBuilder().width(800).height(600).title("Pie chart for companies").build();

        // Customize Chart
        chart.getStyler().setCircular(false);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        for (int i=0; i < feature.size() ; i++) {
        chart.addSeries(feature.get(i), Integer.parseInt(count.get(i)));
        }
        return chart;
        }}