package com.vtrail.terminal;

import android.annotation.SuppressLint;
import android.content.Context;
import android.content.SharedPreferences;
import android.graphics.Bitmap;
import android.net.http.SslError;
import android.os.Bundle;
import android.view.View;
import android.webkit.SslErrorHandler;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.EditText;
import android.widget.Toast;

import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.swiperefreshlayout.widget.SwipeRefreshLayout;

import com.google.android.material.floatingactionbutton.FloatingActionButton;

public class MainActivity extends AppCompatActivity {

    private static final String PREFS_NAME = "VTrailPrefs";
    private static final String KEY_SERVER_URL = "server_url";
    private static final String DEFAULT_URL = "http://192.168.0.106:5000";

    private WebView webView;
    private SwipeRefreshLayout swipeRefresh;
    private FloatingActionButton btnSettings;
    private SharedPreferences prefs;

    @SuppressLint("SetJavaScriptEnabled")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        prefs = getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);

        webView = findViewById(R.id.webView);
        swipeRefresh = findViewById(R.id.swipeRefresh);
        btnSettings = findViewById(R.id.btnSettings);

        WebSettings ws = webView.getSettings();
        ws.setJavaScriptEnabled(true);
        ws.setDomStorageEnabled(true);
        ws.setDatabaseEnabled(true);
        ws.setAllowFileAccess(true);
        ws.setLoadsImagesAutomatically(true);
        ws.setUseWideViewPort(true);
        ws.setLoadWithOverviewMode(true);
        ws.setCacheMode(WebSettings.LOAD_DEFAULT);

        webView.setWebChromeClient(new WebChromeClient());
        webView.setWebViewClient(new WebViewClient() {
            @Override
            public void onPageStarted(WebView view, String url, Bitmap favicon) {
                super.onPageStarted(view, url, favicon);
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                super.onPageFinished(view, url);
                swipeRefresh.setRefreshing(false);
            }

            @Override
            public void onReceivedError(WebView view, WebResourceRequest request, WebResourceError error) {
                super.onReceivedError(view, request, error);
                swipeRefresh.setRefreshing(false);
                if (request.isForMainFrame()) {
                    showErrorDialog("Gagal terhubung ke server: " + getServerUrl());
                }
            }

            @Override
            public void onReceivedSslError(WebView view, SslErrorHandler handler, SslError error) {
                handler.proceed();
            }
        });

        swipeRefresh.setOnRefreshListener(() -> webView.reload());

        btnSettings.setOnClickListener(v -> showUrlDialog());

        // Hide floating button after 5 seconds to keep screen completely immersive
        btnSettings.postDelayed(() -> btnSettings.setVisibility(View.GONE), 6000);

        // Long press anywhere to reveal settings button
        webView.setOnLongClickListener(v -> {
            btnSettings.setVisibility(View.VISIBLE);
            btnSettings.postDelayed(() -> btnSettings.setVisibility(View.GONE), 6000);
            return false;
        });

        loadServerUrl();
    }

    private String getServerUrl() {
        return prefs.getString(KEY_SERVER_URL, DEFAULT_URL);
    }

    private void loadServerUrl() {
        String url = getServerUrl();
        webView.loadUrl(url);
    }

    private void showUrlDialog() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        builder.setTitle(R.string.change_url);
        builder.setMessage(R.string.enter_url);

        final EditText input = new EditText(this);
        input.setText(getServerUrl());
        input.setSingleLine(true);
        builder.setView(input);

        builder.setPositiveButton(R.string.save, (dialog, which) -> {
            String newUrl = input.getText().toString().trim();
            if (!newUrl.isEmpty()) {
                if (!newUrl.startsWith("http://") && !newUrl.startsWith("https://")) {
                    newUrl = "http://" + newUrl;
                }
                prefs.edit().putString(KEY_SERVER_URL, newUrl).apply();
                loadServerUrl();
                Toast.makeText(this, "URL disimpan: " + newUrl, Toast.LENGTH_SHORT).show();
            }
        });

        builder.setNegativeButton(R.string.cancel, (dialog, which) -> dialog.cancel());
        builder.show();
    }

    private void showErrorDialog(String message) {
        new AlertDialog.Builder(this)
                .setTitle("Koneksi Gagal")
                .setMessage(message + "\n\nPastikan PC server menjalankan 'python3 app.py' dan HP berada dalam jaringan Wi-Fi yang sama.")
                .setPositiveButton("Ubah URL", (dialog, which) -> showUrlDialog())
                .setNegativeButton("Coba Lagi", (dialog, which) -> loadServerUrl())
                .show();
    }

    @Override
    public void onBackPressed() {
        if (webView.canGoBack()) {
            webView.goBack();
        } else {
            super.onBackPressed();
        }
    }
}
