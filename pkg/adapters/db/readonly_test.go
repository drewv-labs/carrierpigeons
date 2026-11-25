package db

import "testing"

func TestResolveAllowSuperuserRO(t *testing.T) {
	t.Run("explicit option always wins", func(t *testing.T) {
		t.Setenv("PIGEONS_ALLOW_SUPERUSER_RO", "false")
		t.Setenv("CUSTOM_ALLOW_SUPERUSER_RO", "false")
		if !resolveAllowSuperuserRO(ReadOnlyOptions{AllowSuperuser: true, EnvPrefix: "CUSTOM"}) {
			t.Fatal("expected AllowSuperuser=true to override env vars")
		}
	})

	t.Run("custom env prefix overrides fallback", func(t *testing.T) {
		t.Setenv("ADA_ALLOW_SUPERUSER_RO", "true")
		t.Setenv("PIGEONS_ALLOW_SUPERUSER_RO", "false")
		if !resolveAllowSuperuserRO(ReadOnlyOptions{EnvPrefix: "ADA"}) {
			t.Fatal("expected ADA_ALLOW_SUPERUSER_RO=true to enable superuser RO")
		}
		if !resolveAllowSuperuserRO(ReadOnlyOptions{EnvPrefix: "  ada_ "}) {
			t.Fatal("expected normalized EnvPrefix to check ADA_ALLOW_SUPERUSER_RO")
		}

		t.Setenv("ADA_ALLOW_SUPERUSER_RO", "false")
		t.Setenv("PIGEONS_ALLOW_SUPERUSER_RO", "true")
		if resolveAllowSuperuserRO(ReadOnlyOptions{EnvPrefix: "ADA"}) {
			t.Fatal("expected explicit ADA_ALLOW_SUPERUSER_RO=false to take precedence over PIGEONS_ALLOW_SUPERUSER_RO")
		}
	})

	t.Run("falls back to PIGEONS_ALLOW_SUPERUSER_RO when custom var unset", func(t *testing.T) {
		t.Setenv("ADA_ALLOW_SUPERUSER_RO", "")
		t.Setenv("PIGEONS_ALLOW_SUPERUSER_RO", "true")
		if !resolveAllowSuperuserRO(ReadOnlyOptions{EnvPrefix: "ADA"}) {
			t.Fatal("expected fallback to PIGEONS_ALLOW_SUPERUSER_RO=true when ADA_ALLOW_SUPERUSER_RO is empty")
		}
		if !resolveAllowSuperuserRO(ReadOnlyOptions{}) {
			t.Fatal("expected default EnvPrefix to check PIGEONS_ALLOW_SUPERUSER_RO=true")
		}

		t.Setenv("PIGEONS_ALLOW_SUPERUSER_RO", "")
		if resolveAllowSuperuserRO(ReadOnlyOptions{EnvPrefix: "ADA"}) {
			t.Fatal("expected false when neither custom nor fallback env var is set")
		}
	})
}
