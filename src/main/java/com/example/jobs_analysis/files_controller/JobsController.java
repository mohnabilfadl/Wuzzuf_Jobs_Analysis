package com.example.jobs_analysis.files_controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;


@RestController
public class JobsController {
    JobsDAO service = new JobsDAO();
    @GetMapping("/show_first_records")
    public  String  show_first_records(){
        return service.ShowData();
    }

    @GetMapping("/show_structure")
    public  String  show_structure(){
        return service.structure();
    }


    @GetMapping("/show_summary")
    public  String  show_summary(){
        return service.summary();
    }


    @GetMapping("/show_top_companies")
    public  String  show_top_companies(){
        return service.jobsByCompany();
    }

    @GetMapping("/show_top_titles")
    public  String  show_top_titles(){
        return service.JobsByTitles();
    }

    @GetMapping("/show_top_areas")
    public  String  show_top_countries(){
        return service.JobsByAreas();
    }

    @GetMapping("/show_pie_chart")
    public  String  show_pie_chart() throws IOException {
        return service.pieChartForCompany();
    }

    @GetMapping("/title_bar_chart")
    public  String  title_bar_chart() throws IOException {
        return service.TitlesBarChart();
    }

    @GetMapping("/location_bar_chart")
    public  String  location_bar_chart() throws IOException {
        return service.areasBarChart();
    }

    @GetMapping("/show_top_skills")
    public ResponseEntity<Object> show_top_skills() {
        return service.skill();
    }

    @GetMapping("/show_YearsExp")
    public  String  show_YearsExp()  {
        return service.factYearsExp();
    }


}
