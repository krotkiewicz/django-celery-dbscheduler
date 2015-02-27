try:
    from django.conf.urls import (patterns, include, url,
                                  handler500, handler404)
except ImportError:
    from django.conf.urls.defaults import (patterns, include, url,  # noqa
                                  handler500, handler404)

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns(
    '',
    # Example:
    # (r'^tests/', include('tests.foo.urls')),

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs'
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # (r'^admin/(.*)', admin.site.root),

)
