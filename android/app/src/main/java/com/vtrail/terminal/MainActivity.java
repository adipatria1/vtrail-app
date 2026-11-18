package com.vtrail.terminal;

import android.annotation.SuppressLint;
import android.content.Context;
import android.content.SharedPreferences;
import android.graphics.Bitmap;
import android.net.http.SslError;
import android.os.Bundle;
import android.view.View;
import android.view.WindowManager;
import android.webkit.ConsoleMessage;
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
    public static final String NATIVE_STANDALONE_URL = "file:///android_asset/index.html";

    private WebView webView;
    private SwipeRefreshLayout swipeRefresh;
    private FloatingActionButton btnSettings;
    private SharedPreferences prefs;

    @SuppressLint("SetJavaScriptEnabled")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        // Keep screen alive while trading terminal is active
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
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
        ws.setAllowFileAccessFromFileURLs(true);
        ws.setAllowUniversalAccessFromFileURLs(true);
        ws.setLoadsImagesAutomatically(true);
        ws.setUseWideViewPort(true);
        ws.setLoadWithOverviewMode(true);
        ws.setCacheMode(WebSettings.LOAD_DEFAULT);
        ws.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);

        // Disable pull-to-refresh so user touches/taps on navigation buttons are not hijacked
        swipeRefresh.setEnabled(false);

        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onConsoleMessage(ConsoleMessage consoleMessage) {
                android.util.Log.d("VTrail", consoleMessage.message() + " -- Line "
                        + consoleMessage.lineNumber() + " of "
                        + consoleMessage.sourceId());
                return true;
            }
        });
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
                    showErrorDialog("Koneksi bermasalah ke: " + getServerUrl());
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
        return prefs.getString(KEY_SERVER_URL, NATIVE_STANDALONE_URL);
    }

    private void loadServerUrl() {
        String url = getServerUrl();
        webView.loadUrl(url);
    }

    private void showUrlDialog() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        builder.setTitle("Mode Aplikasi & URL Server");
        builder.setMessage("Pilih mode eksekusi bot:\n- Standalone: Berjalan mandiri 100% di HP (tanpa PC)\n- Remote: Terhubung ke server PC lokal/VPS");

        final EditText input = new EditText(this);
        input.setText(getServerUrl());
        input.setSingleLine(true);
        builder.setView(input);

        builder.setPositiveButton("Simpan URL", (dialog, which) -> {
            String newUrl = input.getText().toString().trim();
            if (!newUrl.isEmpty()) {
                if (!newUrl.startsWith("http://") && !newUrl.startsWith("https://") && !newUrl.startsWith("file:///")) {
                    newUrl = "http://" + newUrl;
                }
                prefs.edit().putString(KEY_SERVER_URL, newUrl).apply();
                loadServerUrl();
                Toast.makeText(this, "URL disimpan: " + newUrl, Toast.LENGTH_SHORT).show();
            }
        });

        builder.setNeutralButton("Reset ke Standalone (HP)", (dialog, which) -> {
            prefs.edit().putString(KEY_SERVER_URL, NATIVE_STANDALONE_URL).apply();
            loadServerUrl();
            Toast.makeText(this, "Mode Standalone HP Aktif (Tanpa PC)", Toast.LENGTH_LONG).show();
        });

        builder.setNegativeButton("Batal", (dialog, which) -> dialog.cancel());
        builder.show();
    }

    private void showErrorDialog(String message) {
        new AlertDialog.Builder(this)
                .setTitle("Koneksi Gagal")
                .setMessage(message + "\n\nIngin kembali ke mode Standalone bawaan HP?")
                .setPositiveButton("Kembali ke Standalone HP", (dialog, which) -> {
                    prefs.edit().putString(KEY_SERVER_URL, NATIVE_STANDALONE_URL).apply();
                    loadServerUrl();
                })
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
