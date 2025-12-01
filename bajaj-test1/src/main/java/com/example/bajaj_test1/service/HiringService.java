package com.example.bajaj_test1.service;

import com.example.bajaj_test1.dto.WebhookResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Service
public class HiringService {

    private final WebClient webClient;

    @Value("${hiring.api.generate}")
    private String generateUrl;

    @Value("${app.regNo}")
    private String regNo;

    public HiringService(WebClient webClient) {
        this.webClient = webClient;
    }

    public void runFlowOnStartup() {

        Map<String, String> body = Map.of(
                "name", "John Doe",
                "regNo", regNo,
                "email", "john@example.com"
        );

        WebhookResponse response = webClient.post()
                .uri(generateUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(WebhookResponse.class)
                .block();

        if (response == null) {
            throw new RuntimeException("generateWebhook returned null");
        }

        String webhook = response.getWebhook();
        String accessToken = response.getAccessToken();

        String finalSql = solveQuestion2(); // because your regNo ends with even number 28

        submitFinalQuery(webhook, accessToken, finalSql);
    }

    private void submitFinalQuery(String webhook, String token, String query) {

        webClient.post()
                .uri(webhook)
                .header(HttpHeaders.AUTHORIZATION, token)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("finalQuery", query))
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(System.out::println)
                .block();
    }

    private String solveQuestion2() {
        return """
WITH high_paid_emps AS (
  SELECT e.emp_id, e.first_name, e.last_name, e.dob, e.department
  FROM employee e
  JOIN payments p ON e.emp_id = p.emp_id
  WHERE p.amount > 70000
  GROUP BY e.emp_id, e.first_name, e.last_name, e.dob, e.department
),
ages AS (
  SELECT
    emp_id,
    first_name,
    last_name,
    department,
    TIMESTAMPDIFF(YEAR, dob, CURDATE()) AS age
  FROM high_paid_emps
),
avg_age_per_dept AS (
  SELECT department AS dept_id,
         AVG(age) AS avg_age
  FROM ages
  GROUP BY department
),
top10_per_dept AS (
  SELECT department AS dept_id,
         CONCAT(first_name, ' ', last_name) AS full_name,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY first_name, last_name) AS rn
  FROM ages
),
list_per_dept AS (
  SELECT dept_id,
         GROUP_CONCAT(full_name ORDER BY full_name SEPARATOR ', ') AS employee_list
  FROM top10_per_dept
  WHERE rn <= 10
  GROUP BY dept_id
)
SELECT
  d.department_name AS DEPARTMENT_NAME,
  ROUND(a.avg_age, 2) AS AVERAGE_AGE,
  COALESCE(l.employee_list, '') AS EMPLOYEE_LIST
FROM department d
LEFT JOIN avg_age_per_dept a ON d.department_id = a.dept_id
LEFT JOIN list_per_dept l ON d.department_id = l.dept_id
ORDER BY d.department_id DESC;
""";
    }
}
